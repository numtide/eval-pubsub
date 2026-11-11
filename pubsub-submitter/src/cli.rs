use std::{net::SocketAddr, path::PathBuf, sync::LazyLock};

use clap::Parser as _;
use prost_types::FileOptions;
use serde::Deserialize;

const DEFAULT_LISTEN_ADDRESSES: LazyLock<Vec<SocketAddr>> = LazyLock::new(|| {
    vec![
        "0.0.0.0:10000".parse().unwrap(),
        "[::]:10000".parse().unwrap(),
    ]
});

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingMissing {
    setting_path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError {
    setting_missing: Vec<SettingMissing>,
}

impl std::error::Error for ConfigError {}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{:?}", self).as_str())
    }
}

impl ConfigError {
    fn empty() -> ConfigError {
        Self {
            setting_missing: Vec::new(),
        }
    }

    pub fn setting_missing<I, S>(&mut self, setting_path: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.setting_missing.push(SettingMissing {
            setting_path: setting_path
                .into_iter()
                .map(|str| str.as_ref().into())
                .collect(),
        });
    }

    pub fn is_empty(&self) -> bool {
        self.setting_missing.is_empty()
    }
}

#[derive(clap::Parser, Debug, Eq, Clone, PartialEq)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long, env = "PUBSUB_CONFIG_FILE")]
    pub config_file: Option<PathBuf>,
    #[arg(long, env = "PUBSUB_LISTEN")]
    pub listen_address: Option<Vec<SocketAddr>>,
    #[arg(long, env = "PUBSUB_DATABASE_URL")]
    pub database_url: Option<String>,
}

#[derive(Deserialize)]
pub struct Config {
    #[serde(alias = "listenAddress")]
    pub listen_address: Option<Vec<SocketAddr>>,
    #[serde(alias = "databaseUrl")]
    pub database_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalConfig {
    pub listen_address: Vec<SocketAddr>,
    pub database_url: String,
}

impl FinalConfig {
    pub fn parse() -> Result<FinalConfig, Box<dyn std::error::Error + Send + Sync>> {
        let cli: Cli = Cli::parse();
        Self::merge(
            cli.config_file
                .as_ref()
                .map(|path| {
                    serde_json::from_reader(std::fs::File::open(path)?)
                        .map_err(Into::<Box<dyn std::error::Error + Send + Sync>>::into)
                })
                .transpose()?,
            cli,
        )
        .map_err(Into::into)
    }

    pub fn merge(config: Option<Config>, cli: Cli) -> Result<FinalConfig, ConfigError> {
        let mut errors = ConfigError::empty();

        let listen_address: Option<&_> = cli.listen_address.as_ref().or(config
            .as_ref()
            .and_then(|config| config.listen_address.as_ref()));
        let database_url = cli.database_url.as_ref().or(config
            .as_ref()
            .and_then(|config| config.database_url.as_ref()));

        if listen_address.is_none() {
            errors.setting_missing(&["listen_address"]);
        }
        if database_url.is_none() {
            errors.setting_missing(&["database_url"]);
        }

        if errors.is_empty() {
            Ok(FinalConfig {
                listen_address: listen_address.unwrap().clone(),
                database_url: database_url.unwrap().clone(),
            })
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_has_priority() {
        let cli = Cli {
            config_file: None,
            listen_address: Some(Vec::from(&["0.0.0.0:1000".parse().unwrap()])),
            database_url: Some("dbname=pubsub".into()),
        };

        let config = Config {
            listen_address: Some(Vec::from(&["[::]:1000".parse().unwrap()])),
            database_url: Some("dbname=submitter".into()),
        };

        let final_config = FinalConfig::merge(Some(config), cli);

        assert_eq!(
            final_config,
            Ok(FinalConfig {
                listen_address: Vec::from(&["0.0.0.0:1000".parse().unwrap()]),
                database_url: "dbname=pubsub".into()
            })
        );
    }

    #[test]
    fn config_is_enough() {
        let cli = Cli {
            config_file: None,
            listen_address: None,
            database_url: None,
        };

        let config = Config {
            listen_address: Some(Vec::from(&["0.0.0.0:1000".parse().unwrap()])),
            database_url: Some("dbname=pubsub".into()),
        };

        let final_config = FinalConfig::merge(Some(config), cli);

        assert_eq!(
            final_config,
            Ok(FinalConfig {
                listen_address: Vec::from(&["0.0.0.0:1000".parse().unwrap()]),
                database_url: "dbname=pubsub".into()
            })
        );
    }

    #[test]
    fn missing_settings_in_both_are_caught() {
        let cli = Cli {
            config_file: None,
            listen_address: None,
            database_url: None,
        };

        let config = Config {
            listen_address: None,
            database_url: Some("dbname=pubsub".into()),
        };

        let final_config = FinalConfig::merge(Some(config), cli);

        let mut expected_error = ConfigError::empty();
        expected_error.setting_missing(&["listen_address"]);
        assert_eq!(final_config, Err(expected_error));
    }
}
