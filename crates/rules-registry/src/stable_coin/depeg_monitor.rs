//! Poll rule: a stablecoin trades outside `1 ± depeg_threshold_bps` on a watched
//! DEX pool for `confirmation_polls` consecutive polls.

use crate::stable_coin::parser::Pool;
use crate::stable_coin::{DEFAULT_POLL_INTERVAL, PROGRAM};
use anyhow::Context;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::Deserialize;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_sentinel::utility::pubkey::{deserialize_pubkey, deserialize_pubkey_vec};
use solana_sentinel::utility::rpc::try_fetch_accounts_with_slot;
use solana_sentinel::{Alert, BootstrapCtx, Deps, PollCtx, PollRule, RuleId, Severity};
use spl_token_interface::state::Mint;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{info, warn};

const BPS_PER_UNIT: f64 = 10_000.0;

#[derive(Debug, Deserialize)]
pub struct StableCoinConfig {
    #[serde(deserialize_with = "deserialize_pubkey")]
    pub mint: Pubkey,
    /// Name used in alerts, e.g. "JUPUSD".
    pub ticker: String,
    pub depeg_threshold_bps: u32,
    /// Pools pairing this coin with a USD-pegged quote asset; each is priced independently.
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub pools: Vec<Pubkey>,
}

#[derive(Debug, Deserialize)]
pub struct DepegMonitorConfig {
    /// Written as `[[stable_coins]]` tables, indexed by mint on load.
    #[serde(deserialize_with = "stable_coins_by_mint")]
    pub stable_coins: HashMap<Pubkey, StableCoinConfig>,
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,
    /// Consecutive out-of-band polls before alerting, so one transient print pages no one.
    #[serde(default = "default_confirmation_polls")]
    pub confirmation_polls: u32,
}

fn default_poll_interval_seconds() -> u64 {
    DEFAULT_POLL_INTERVAL.as_secs()
}

fn default_confirmation_polls() -> u32 {
    3
}

fn stable_coins_by_mint<'de, D>(de: D) -> Result<HashMap<Pubkey, StableCoinConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mut coins = HashMap::new();
    for coin in Vec::<StableCoinConfig>::deserialize(de)? {
        if coins.insert(coin.mint, coin).is_some() {
            return Err(serde::de::Error::custom(
                "stable coin mint listed more than once",
            ));
        }
    }
    Ok(coins)
}

#[derive(Debug)]
pub struct DepegMonitor {
    config: DepegMonitorConfig,
    pool_to_mint: HashMap<Pubkey, Pubkey>,
    /// Mint decimals never change; filled once at bootstrap.
    decimals: HashMap<Pubkey, u8>,
    /// Consecutive out-of-band polls per pool; reset once it is back in band.
    strikes: Mutex<HashMap<Pubkey, u32>>,
}

impl DepegMonitor {
    /// Raydium carries decimals in the pool; the others take the bootstrap cache.
    fn with_decimals(&self, pool: &Pool) -> Pool {
        let mut pool = pool.clone();
        pool.base_decimals = pool
            .base_decimals
            .or_else(|| self.decimals.get(&pool.base_mint).copied());
        pool.quote_decimals = pool
            .quote_decimals
            .or_else(|| self.decimals.get(&pool.quote_mint).copied());
        pool
    }

    /// Count this poll against the pool; true once the streak reaches the threshold.
    fn confirmed(&self, pool: &Pubkey) -> bool {
        let mut strikes = self.strikes.lock();
        let count = strikes.entry(*pool).or_insert(0);
        *count = count.saturating_add(1);
        *count >= self.config.confirmation_polls
    }
}

#[async_trait]
impl PollRule for DepegMonitor {
    const ID: RuleId = RuleId::new(PROGRAM, "depeg_monitor");
    type Config = DepegMonitorConfig;
    type Item = Pool;

    fn accounts(cfg: &Self::Config) -> HashSet<Pubkey> {
        cfg.stable_coins
            .values()
            .flat_map(|coin| coin.pools.iter().copied())
            .collect()
    }

    fn poll_interval(cfg: &Self::Config) -> Option<Duration> {
        Some(Duration::from_secs(cfg.poll_interval_seconds))
    }

    fn build(config: Self::Config, _deps: &Deps) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !config.stable_coins.is_empty(),
            "stable_coins must not be empty"
        );
        anyhow::ensure!(
            config.confirmation_polls > 0,
            "confirmation_polls must be at least 1"
        );
        let mut pool_to_mint = HashMap::new();
        for (mint, coin) in &config.stable_coins {
            anyhow::ensure!(
                coin.depeg_threshold_bps > 0,
                "{} ({mint}) has depeg_threshold_bps = 0",
                coin.ticker
            );
            anyhow::ensure!(
                !coin.pools.is_empty(),
                "{} ({mint}) has no pools",
                coin.ticker
            );
            for pool in &coin.pools {
                anyhow::ensure!(
                    pool_to_mint.insert(*pool, *mint).is_none(),
                    "pool {pool} is listed under more than one stable coin"
                );
            }
        }
        Ok(Self {
            config,
            pool_to_mint,
            decimals: HashMap::new(),
            strikes: Mutex::new(HashMap::new()),
        })
    }

    /// Fail startup if a pool is missing, undecodable or lacks its coin; cache mint decimals.
    async fn bootstrap(&mut self, ctx: &BootstrapCtx) -> anyhow::Result<()> {
        let pubkeys: Vec<Pubkey> = self.pool_to_mint.keys().copied().collect();
        let (accounts, _slot) = try_fetch_accounts_with_slot(&ctx.rpc, &pubkeys, None).await?;

        let mut mints = HashSet::new();
        for (pool_pubkey, mint) in &self.pool_to_mint {
            let account = accounts
                .get(pool_pubkey)
                .with_context(|| format!("pool {pool_pubkey} not found on-chain"))?;
            let pool = Pool::parse(*pool_pubkey, account)?;
            anyhow::ensure!(
                pool.contains(mint),
                "pool {pool_pubkey} ({}) does not contain mint {mint}",
                pool.dex
            );
            mints.extend([pool.base_mint, pool.quote_mint]);
        }

        let mints: Vec<Pubkey> = mints.into_iter().collect();
        let (mint_accounts, _slot) = try_fetch_accounts_with_slot(&ctx.rpc, &mints, None).await?;
        for (pubkey, account) in mint_accounts {
            // Token-2022 mints share the base layout; extensions follow it.
            if let Some(mint) = account
                .data
                .get(..Mint::LEN)
                .and_then(|base| Mint::unpack_from_slice(base).ok())
            {
                self.decimals.insert(pubkey, mint.decimals);
            }
        }
        let missing: Vec<_> = mints
            .iter()
            .filter(|m| !self.decimals.contains_key(m))
            .collect();
        anyhow::ensure!(
            missing.is_empty(),
            "could not read decimals for mints {missing:?}"
        );
        info!(
            pools = pubkeys.len(),
            mints = mints.len(),
            "depeg_monitor bootstrapped"
        );
        Ok(())
    }

    fn evaluate(&self, pool: &Pool, _ctx: &PollCtx) -> Option<Alert> {
        let mint = self.pool_to_mint.get(&pool.pubkey)?;
        let coin = self.config.stable_coins.get(mint)?;
        // An empty CLMM pool still reports a price, but nothing trades at it.
        if pool.liquidity == Some(0) {
            return None;
        }
        let pool = self.with_decimals(pool);
        let price = pool.price_of(mint)?;

        let deviation_bps = (price - 1.0) * BPS_PER_UNIT;
        if deviation_bps.abs() <= f64::from(coin.depeg_threshold_bps) {
            self.strikes.lock().remove(&pool.pubkey);
            return None;
        }
        if !self.confirmed(&pool.pubkey) {
            warn!(ticker = coin.ticker, pool = %pool.pubkey, price, "pool out of band; confirming");
            return None;
        }

        let direction = if deviation_bps < 0.0 {
            "below"
        } else {
            "above"
        };
        let quote = pool.counterpart(mint)?;
        Some(
            Alert::for_rule(Self::ID)
                .severity(Severity::Critical)
                .message(format!(
                    "{} has depegged: trading at {price:.4} on {} pool {} ({:.2}% {direction} \
                     peg, threshold {:.2}%)",
                    coin.ticker,
                    pool.dex,
                    pool.pubkey,
                    deviation_bps.abs() / 100.0,
                    f64::from(coin.depeg_threshold_bps) / 100.0,
                ))
                .meta("ticker", &coin.ticker)
                .meta("mint", mint)
                .meta("pool", pool.pubkey)
                .meta("dex", pool.dex)
                .meta("quote_mint", quote)
                .meta("price", format!("{price:.6}"))
                .meta("deviation_bps", format!("{deviation_bps:.2}"))
                .meta("threshold_bps", coin.depeg_threshold_bps)
                .key([coin.ticker.clone(), pool.pubkey.to_string()])
                .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stable_coin::parser::Dex;
    use solana_sentinel::testing::{config_from_toml, poll_ctx};

    const JUPUSD: &str = "JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD";
    const POOL: &str = "4tgA77vUnDaTADqGJYhJYT1Qf8nnukxFGohoXR3tzPau";

    fn pool_at(price: f64) -> Pool {
        Pool {
            pubkey: Pubkey::from_str_const(POOL),
            dex: Dex::OrcaWhirlpool,
            base_mint: Pubkey::from_str_const(JUPUSD),
            quote_mint: Pubkey::new_unique(),
            raw_price: price,
            base_decimals: Some(6),
            quote_decimals: Some(6),
            liquidity: Some(1),
        }
    }

    // Out of band alerts only after `confirmation_polls` polls; back in band resets.
    #[test]
    fn toml_config_to_alert() {
        let cfg = config_from_toml(&format!(
            "confirmation_polls = 2\n[[stable_coins]]\nmint = \"{JUPUSD}\"\nticker = \
             \"JUPUSD\"\ndepeg_threshold_bps = 50\npools = [\"{POOL}\"]\n"
        ));
        let rule = DepegMonitor::build(cfg, &Deps::default()).unwrap();
        let ctx = poll_ctx(1);

        assert!(rule.evaluate(&pool_at(0.9995), &ctx).is_none());
        assert!(rule.evaluate(&pool_at(0.99), &ctx).is_none());
        let alert = rule.evaluate(&pool_at(0.99), &ctx).unwrap();
        assert_eq!(alert.metadata["deviation_bps"], "-100.00");
        assert_eq!(alert.metadata["dex"], "Orca Whirlpool");

        assert!(rule.evaluate(&pool_at(1.0), &ctx).is_none());
        assert!(rule.evaluate(&pool_at(0.99), &ctx).is_none());
    }
}
