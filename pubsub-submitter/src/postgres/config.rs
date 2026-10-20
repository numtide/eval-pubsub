#![allow(unstable_name_collisions)]
use std::net::IpAddr;

use itertools::Itertools;
use tokio_postgres::{
    Config,
    config::{ChannelBinding, Host, LoadBalanceHosts, SslMode, SslNegotiation, TargetSessionAttrs},
};

trait ConfigString {
    fn to_key_value(&self) -> Vec<(String, String)>;
    fn to_key_value_string(&self) -> String;
}

impl ConfigString for Config {
    fn to_key_value(&self) -> Vec<(String, String)> {
        let user = self.get_user().map(|user| ("user", user));
        let password = self.get_password().map(|password| {
            (
                "password",
                std::str::from_utf8(password).expect("non UTF-8 password"),
            )
        });
        let dbname = self.get_dbname().map(|dbname| ("dbname", dbname));
        let options = self.get_options().map(|options| ("options", options));
        let application_name = self
            .get_application_name()
            .map(|application_name| ("application_name", application_name));
        let ssl_mode = Some((
            "sslmode",
            match self.get_ssl_mode() {
                SslMode::Disable => "disable",
                SslMode::Prefer => "prefer",
                SslMode::Require => "require",
                _ => panic!("some other ssl mode"),
            },
        ));
        let ssl_negotiation = Some((
            "sslnegotiation",
            match self.get_ssl_negotiation() {
                SslNegotiation::Postgres => "postgres",
                SslNegotiation::Direct => "direct",
                _ => panic!("some other ssl negotiation"),
            },
        ));
        let hosts = self
            .get_hosts()
            .iter()
            .map(|host| match host {
                Host::Tcp(tcp_hostname) => tcp_hostname.as_ref(),
                Host::Unix(path) => path.as_path().to_str().expect("non UTF-8 path"),
            })
            .intersperse(",")
            .collect::<String>();
        let hostaddrs = self
            .get_hostaddrs()
            .iter()
            .map(|hostaddr| match hostaddr {
                IpAddr::V4(ipv4) => format!("{ipv4}"),
                IpAddr::V6(ipv6) => format!("{ipv6}"),
            })
            .intersperse(",".to_owned())
            .collect::<String>();
        let ports = self
            .get_ports()
            .iter()
            .map(|port| port.to_string())
            .intersperse(",".to_owned())
            .collect::<String>();
        let connect_timeout = self
            .get_connect_timeout()
            .map(|connect_timeout| connect_timeout.as_secs().to_string());
        let tcp_user_timeout = self
            .get_tcp_user_timeout()
            .map(|tcp_user_timeout| tcp_user_timeout.as_secs().to_string());
        let keepalives = self.get_keepalives();
        let keepalives_idle = self.get_keepalives_idle().as_secs().to_string();
        let keepalives_interval = self
            .get_keepalives_interval()
            .map(|keepalives_interval| keepalives_interval.as_secs().to_string());
        let keepalives_retries = self
            .get_keepalives_retries()
            .map(|keepalive_retries| keepalive_retries.to_string());
        let target_session_attrs = self.get_target_session_attrs();
        let channel_binding = self.get_channel_binding();
        let load_balance_hosts = self.get_load_balance_hosts();

        ([
            user,
            password,
            dbname,
            options,
            application_name,
            ssl_mode,
            ssl_negotiation,
            Some(("host", hosts.as_ref())),
            Some(("hostaddr", hostaddrs.as_ref())),
            Some(("port", ports.as_ref())),
            connect_timeout
                .as_ref()
                .map(|connect_timeout| ("connect_timeout", connect_timeout.as_ref())),
            tcp_user_timeout
                .as_ref()
                .map(|tcp_user_timeout| ("tcp_user_timeout", tcp_user_timeout.as_ref())),
            if keepalives {
                Some(("keepalives", "1"))
            } else {
                Some(("keepalives", "0"))
            },
            Some(("keepalives_idle", keepalives_idle.as_ref())),
            keepalives_interval
                .as_ref()
                .map(|keepalives_interval| ("keepalives_interval", keepalives_interval.as_ref())),
            keepalives_retries
                .as_ref()
                .map(|keepalives_retries| ("keepalives_retries", keepalives_retries.as_ref())),
            Some((
                "target_session_attrs",
                match target_session_attrs {
                    TargetSessionAttrs::Any => "any",
                    TargetSessionAttrs::ReadWrite => "read-write",
                    TargetSessionAttrs::ReadOnly => "read-only",
                    _ => panic!("unknown target session attrs"),
                },
            )),
            Some((
                "channel_binding",
                match channel_binding {
                    ChannelBinding::Disable => "disable",
                    ChannelBinding::Prefer => "prefer",
                    ChannelBinding::Require => "require",
                    _ => panic!("unknown channel binding"),
                },
            )),
            Some((
                "load_balance_hosts",
                match load_balance_hosts {
                    LoadBalanceHosts::Disable => "disable",
                    LoadBalanceHosts::Random => "random",
                    _ => panic!("unknown load balance hosts"),
                },
            )),
        ])
        .into_iter()
        .map(|kv| match kv {
            Some((_k, v)) if v == "" => None,
            _ => kv,
        })
        .flatten()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    fn to_key_value_string(&self) -> String {
        self.to_key_value()
            .iter()
            .map(|(key, value)| {
                if value.contains(' ') {
                    format!("{key}='{value}'")
                } else {
                    format!("{key}={value}")
                }
            })
            .intersperse(" ".to_owned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    #[test]
    fn roundtrip() {
        let connstr = "user=aaaa password=bbbb dbname=cccc options='-d -d -d -d' application_name=eeee sslmode=require sslnegotiation=postgres host=host1,host2 hostaddr=127.0.0.1,127.0.0.2 port=5432,5433 connect_timeout=20 tcp_user_timeout=20 keepalives=1 keepalives_idle=20 keepalives_interval=20 keepalives_retries=5 target_session_attrs=any channel_binding=require load_balance_hosts=random";

        let config = Config::from_str(connstr).unwrap();

        let connstr_roundtrip = config.to_key_value_string();

        assert_eq!(connstr, connstr_roundtrip);
    }
}
