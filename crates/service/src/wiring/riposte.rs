//! [`RiposteService`]: builds [`Riposte`] from `[riposte]` and the rules with `riposte = true`,
//! then runs it. Built before the stream connects, so a bad signer (incl. an unusable KMS key)
//! fails startup.

use super::{connect_to_redis_cluster, known_rules};
use crate::config::SentinelConfig;
use anyhow::{bail, Context};
use data_source::rpc::build_rpc_client;
use dedup_gate::fleet_gate::FleetGate;
use domain::alert::Alert;
use domain::counter_measure::Trigger;
use domain::programs::Program;
use domain::rules::RuleId;
use riposte::balance::spawn_balance_watch;
use riposte::blockhash::BlockhashCache;
use riposte::broadcast::{RpcBroadcaster, SimulateBroadcaster};
use riposte::config::{BroadcasterConfig, Mode, RiposteConfig, SignerConfig};
use riposte::fees::PriorityFeeCache;
use riposte::halt::HaltSwitch;
use riposte::signer::{KeypairSigner, KmsSigner};
use riposte::traits::{Broadcaster, TxSigner};
use riposte::Riposte;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sentinel::{RiposteCtx, ValidatedProgram};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

/// Landing takes up to ~90 s (a ~60 s blockhash plus finalized lag); twice that for margin.
const MIN_COOLDOWN_SECONDS: u64 = 180;

/// Riposte, built and not yet running, with the queue rules feed and their [`RiposteCtx`].
pub struct RiposteService {
    riposte: Riposte,
    queue: mpsc::Receiver<Trigger>,
    ctx: RiposteCtx,
    cancel: CancellationToken,
}

impl RiposteService {
    /// Drains the queue until cancelled; the returned [`RiposteCtx`] goes to the rules (via
    /// `BootstrapCtx`), so their `counteract` runs and lands in that queue.
    pub fn start(self, tracker: &TaskTracker) -> RiposteCtx {
        self.riposte.start(self.queue, self.cancel, tracker);
        self.ctx
    }

    /// `None` when Riposte is off or no rule has `riposte = true`. Outcome alerts go to `alerts`.
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        config: &SentinelConfig,
        validated: &[ValidatedProgram],
        rpc: &Arc<RpcClient>,
        alerts: mpsc::Sender<Alert>,
        pod_id: &str,
        service_name: &str,
        cancel: &CancellationToken,
        tracker: &TaskTracker,
    ) -> anyhow::Result<Option<Self>> {
        let rules = riposte_rules(config, validated)?;
        // Absent behaves like `enabled = false`: rules may keep riposte = true, nothing is sent.
        let Some(cfg) = config.riposte() else {
            if !rules.is_empty() {
                warn!(
                    rules = rules.len(),
                    "rules set riposte = true but [riposte] is absent; riposte not started"
                );
            }
            return Ok(None);
        };
        let fee = cfg.fee;
        if !(1..=10_000).contains(&fee.percentile_bps)
            || fee.min_micro_lamports > fee.max_micro_lamports
        {
            bail!("[riposte.fee] needs percentile_bps in 1..=10000 and min_micro_lamports <= max");
        }
        // The claim must outlive landing (sign + up to a blockhash lifetime of rebroadcasts), or
        // another pod could claim the same condition while the first tx is still in flight.
        if cfg.cooldown_seconds < MIN_COOLDOWN_SECONDS {
            bail!("[riposte] cooldown_seconds must be >= {MIN_COOLDOWN_SECONDS}");
        }
        let fleet_url = config.redis().redis_fleet_url.as_deref();
        if cfg.mode == Mode::Live && fleet_url.is_none() {
            bail!(
                "riposte mode = \"live\" needs redis_fleet_url, so pods never send the same \
                 countermeasure twice"
            );
        }
        // Checked even when disabled, so a missing signer fails startup before it's turned on.
        let signer_names = program_signers(cfg, &rules)?;
        if !cfg.enabled || rules.is_empty() {
            info!(enabled = cfg.enabled, "riposte not started");
            return Ok(None);
        }

        let signers = build_signers(cfg, &signer_names).await?;
        let cooldown = Duration::from_secs(cfg.cooldown_seconds);
        let periods = rules.iter().map(|rule| (*rule, cooldown)).collect();
        // Apart from alerting's keys, and dry-run claims never suppress a live countermeasure.
        let mode = match cfg.mode {
            Mode::DryRun => "dry_run",
            Mode::Live => "live",
        };
        let namespace = format!("{service_name}:riposte:{mode}");
        let (fleet, halt) = match fleet_url {
            Some(url) => {
                let conn = connect_to_redis_cluster(url)
                    .await
                    .context("riposte gate")?;
                // One key for both modes: halting stops dry run and live alike.
                let halt_key = format!("{service_name}:riposte:halt");
                info!(
                    halt_key,
                    "riposte kill switch: SET it in the fleet Redis to stop all countermeasures"
                );
                let halt = HaltSwitch::spawn(conn.clone(), halt_key, cancel.clone(), tracker);
                (FleetGate::new(conn, periods, &namespace, pod_id), halt)
            }
            None => {
                warn!(
                    "No redis_fleet_url; dry-run riposte dedups within this process only and has \
                     no kill switch"
                );
                (FleetGate::in_memory(periods), HaltSwitch::default())
            }
        };
        info!(mode = ?cfg.mode, rules = rules.len(), "riposte enabled");
        // Low balance alerts on one of the program's responding rules, so it reaches its sinks.
        let watched = signers
            .iter()
            .filter_map(|(p, s)| {
                let rule = rules
                    .iter()
                    .filter(|r| r.program == *p)
                    .min_by_key(|r| (r.name, r.instance))?;
                Some((*rule, s.pubkey()))
            })
            .collect();
        spawn_balance_watch(
            Arc::clone(rpc),
            watched,
            cfg.min_balance_lamports,
            alerts.clone(),
            cancel.clone(),
            tracker,
        );
        let (tx, queue) = mpsc::channel(riposte::QUEUE);
        let ctx = RiposteCtx {
            authorities: signers.iter().map(|(p, s)| (*p, s.pubkey())).collect(),
            rules,
            tx,
        };
        let riposte = Riposte {
            mode: cfg.mode,
            signers,
            broadcasters: build_broadcasters(cfg, rpc)?,
            rpc: Arc::clone(rpc),
            blockhash: BlockhashCache::spawn(Arc::clone(rpc), cancel.clone(), tracker),
            fees: PriorityFeeCache::spawn(cfg.fee, Arc::clone(rpc), cancel.clone(), tracker),
            allowed_programs: cfg.allowed_programs.clone(),
            gate: fleet.strict(),
            halt,
            alerts,
        };
        Ok(Some(Self {
            riposte,
            queue,
            ctx,
            cancel: cancel.clone(),
        }))
    }
}

/// Rules with `riposte = true`, resolved against the validated ones.
fn riposte_rules(
    config: &SentinelConfig,
    validated: &[ValidatedProgram],
) -> anyhow::Result<HashSet<RuleId>> {
    let known = known_rules(validated);
    config
        .programs_with_rules()
        .iter()
        .flat_map(|p| p.rules.iter().filter(|r| r.riposte).map(move |r| (p, r)))
        .map(|(p, r)| {
            // Outcome alerts go to the rule's sinks; without any, nobody would hear of them.
            if r.sinks.is_empty() {
                bail!(
                    "program `{}` rule `{}` has riposte = true but no sinks",
                    p.program,
                    r.label()
                );
            }
            known
                .get(&(p.program.as_str(), r.name.as_str(), r.instance.as_deref()))
                .copied()
                .with_context(|| {
                    format!("program `{}` rule `{}` not validated", p.program, r.label())
                })
        })
        .collect()
}

/// Signer name per program that has a `riposte = true` rule; each must be in
/// `[riposte.programs]` and name an entry of `[riposte.signers]`.
fn program_signers<'a>(
    cfg: &'a RiposteConfig,
    rules: &HashSet<RuleId>,
) -> anyhow::Result<HashMap<Program, &'a str>> {
    let mut out = HashMap::new();
    for program in rules.iter().map(|r| r.program) {
        let name = cfg.programs.get(program.as_str()).with_context(|| {
            format!(
                "program `{program}` has riposte = true rules but no signer in [riposte.programs]"
            )
        })?;
        if !cfg.signers.contains_key(name) {
            bail!("[riposte.programs] {program} = \"{name}\" is not in [riposte.signers]");
        }
        out.insert(program, name.as_str());
    }
    Ok(out)
}

/// One signer per name, shared by the programs that name it.
async fn build_signers(
    cfg: &RiposteConfig,
    names: &HashMap<Program, &str>,
) -> anyhow::Result<HashMap<Program, Arc<dyn TxSigner>>> {
    let mut built: HashMap<&str, Arc<dyn TxSigner>> = HashMap::new();
    let mut out = HashMap::with_capacity(names.len());
    for (program, name) in names {
        if !built.contains_key(name) {
            let signer: Arc<dyn TxSigner> = match &cfg.signers[*name] {
                SignerConfig::AwsKms { key_id, region } => Arc::new(
                    KmsSigner::connect(key_id, region.as_deref())
                        .await
                        .with_context(|| format!("riposte signer `{name}`"))?,
                ),
                SignerConfig::KeypairFile { path } => Arc::new(
                    KeypairSigner::from_file(path)
                        .with_context(|| format!("riposte signer `{name}`"))?,
                ),
            };
            built.insert(name, signer);
        }
        let signer = Arc::clone(&built[name]);
        info!(%program, signer = name, authority = %signer.pubkey(), "riposte signer ready");
        out.insert(*program, signer);
    }
    Ok(out)
}

fn build_broadcasters(
    cfg: &RiposteConfig,
    rpc: &Arc<RpcClient>,
) -> anyhow::Result<Vec<Arc<dyn Broadcaster>>> {
    if cfg.mode == Mode::DryRun {
        return Ok(vec![Arc::new(SimulateBroadcaster::new(Arc::clone(rpc)))]);
    }
    if cfg.broadcasters.is_empty() {
        bail!("riposte mode = \"live\" needs at least one [[riposte.broadcasters]]");
    }
    cfg.broadcasters
        .iter()
        .map(|b| match b {
            BroadcasterConfig::Rpc { id, url, headers } => {
                let rpc = build_rpc_client(url.as_str(), headers)
                    .with_context(|| format!("riposte broadcaster `{id}`"))?;
                Ok(Arc::new(RpcBroadcaster::new(id.clone(), rpc)) as Arc<dyn Broadcaster>)
            }
        })
        .collect()
}
