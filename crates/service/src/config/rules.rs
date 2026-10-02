use anyhow::{bail, Context};
use config::{Config, File, FileFormat, Value};
use domain::sinks::SinkId;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct RuleConfig {
    pub program: String,
    pub name: String,
    pub sinks: Vec<SinkId>,
    pub cooldown_period_seconds: Option<u64>,
    /// The rule's typed config table; may be omitted for rules that take none.
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RulesLayout {
    #[serde(default)]
    rules: Vec<RuleConfig>,
}

pub struct RulesConfig(pub Vec<RuleConfig>);

impl RulesConfig {
    /// `RULES_FILE` names one file or a directory; a directory is every `*.toml` in it,
    /// in name order. Without it `./rules.toml` is used if present, else `./rules/`.
    pub fn load() -> anyhow::Result<Self> {
        let path = match std::env::var("RULES_FILE") {
            Ok(p) => PathBuf::from(p),
            Err(_) if Path::new("./rules.toml").is_file() => PathBuf::from("./rules.toml"),
            Err(_) => PathBuf::from("./rules"),
        };
        Self::load_path(&path)
    }

    pub fn load_path(path: &Path) -> anyhow::Result<Self> {
        let files = if path.is_dir() {
            rule_files(path)?
        } else {
            vec![path.to_path_buf()]
        };
        if files.is_empty() {
            bail!("no *.toml rule files found in `{}`", path.display());
        }
        let mut rules = Vec::new();
        for file in files {
            let parsed = Config::builder()
                .add_source(File::from(file.as_path()).format(FileFormat::Toml))
                .build()
                .and_then(Config::try_deserialize::<RulesLayout>)
                .with_context(|| format!("failed to load rules file `{}`", file.display()))?;
            tracing::info!(file = %file.display(), rules = parsed.rules.len(), "loaded rules file");
            rules.extend(parsed.rules);
        }
        Ok(Self(rules))
    }
}

fn rule_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read rules dir `{}`", dir.display()))?;
    let mut files: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|ext| ext == "toml"))
        .collect();
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_merges_rules_in_file_name_order() {
        let dir = std::env::temp_dir().join(format!("sentinel-rules-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("b_lend.toml"),
            "[[rules]]\nprogram = \"lend\"\nname = \"solvency\"\nsinks = [\"slack\"]\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("a_general.toml"),
            "[[rules]]\nprogram = \"loader\"\nname = \"upgrade\"\nsinks = [\"slack\"]\n",
        )
        .unwrap();
        std::fs::write(dir.join("notes.md"), "ignored").unwrap();

        let loaded = RulesConfig::load_path(&dir).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();

        let names: Vec<(&str, &str)> = loaded
            .0
            .iter()
            .map(|r| (r.program.as_str(), r.name.as_str()))
            .collect();
        assert_eq!(names, vec![("loader", "upgrade"), ("lend", "solvency")]);
    }
}
