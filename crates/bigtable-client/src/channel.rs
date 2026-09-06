use http::Uri;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, channel::Change};

use crate::{ClientConfig, ConfigField, ConfigIssue, Error};

pub(crate) async fn connect(config: &ClientConfig) -> Result<Channel, Error> {
    let endpoint = build_endpoint(config)?;
    if config.channel_pool_size() == 1 {
        return endpoint.connect().await.map_err(|source| Error::Transport {
            source: Box::new(source),
        });
    }

    endpoint
        .clone()
        .connect()
        .await
        .map_err(|source| Error::Transport {
            source: Box::new(source),
        })?;

    let (channel, sender) = Channel::balance_channel(config.channel_pool_size());
    for index in 0..config.channel_pool_size() {
        sender
            .send(Change::Insert(index, endpoint.clone()))
            .await
            .map_err(|_| Error::ChannelPoolClosed)?;
    }
    Ok(channel)
}

fn build_endpoint(config: &ClientConfig) -> Result<Endpoint, Error> {
    let uri = config
        .service_endpoint()
        .parse::<Uri>()
        .map_err(|_| Error::InvalidConfig {
            field: if config.uses_emulator() {
                ConfigField::EmulatorHost
            } else {
                ConfigField::Endpoint
            },
            issue: ConfigIssue::InvalidUri,
        })?;
    let mut endpoint = Endpoint::from(uri)
        .connect_timeout(config.connect_timeout())
        .http2_keep_alive_interval(config.keep_alive_interval())
        .keep_alive_timeout(config.keep_alive_timeout())
        .keep_alive_while_idle(true)
        .tcp_nodelay(true);

    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots())
            .map_err(|source| Error::Transport {
                source: Box::new(source),
            })?;
    }

    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, time::Duration};

    use tokio::io::AsyncReadExt;

    use super::{build_endpoint, connect};
    use crate::{ClientConfig, Error};

    #[test]
    fn endpoint_uses_configured_uri_and_transport_settings() {
        let config = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_emulator_host("localhost:8086")
            .expect("valid emulator")
            .with_connect_timeout(Duration::from_secs(2))
            .expect("valid timeout");

        let endpoint = build_endpoint(&config).expect("valid endpoint");

        assert_eq!(endpoint.uri().to_string(), "http://localhost:8086/");
        assert_eq!(endpoint.get_connect_timeout(), Some(Duration::from_secs(2)));
        assert!(endpoint.get_tcp_nodelay());
    }

    #[test]
    fn production_endpoint_enables_tls() {
        let config = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_endpoint("https://example.test")
            .expect("valid endpoint");

        let endpoint = build_endpoint(&config).expect("valid TLS endpoint");

        assert_eq!(endpoint.uri().to_string(), "https://example.test/");
    }

    #[tokio::test]
    async fn https_emulator_starts_a_tls_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("available local port");
        let address = listener.local_addr().expect("bound address");
        let config = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_emulator_host(format!("https://{address}"))
            .expect("valid emulator")
            .with_connect_timeout(Duration::from_secs(2))
            .expect("valid timeout");

        let observe_handshake = async {
            let (mut socket, _) = listener.accept().await.expect("client connects");
            let mut prefix = [0; 3];
            socket.read_exact(&mut prefix).await.expect("TLS record");
            // TLS handshake record and legacy record version in a ClientHello.
            assert_eq!(prefix, [0x16, 0x03, 0x01]);
        };
        let (result, observed) = tokio::join!(
            connect(&config),
            tokio::time::timeout(Duration::from_secs(2), observe_handshake),
        );

        observed.expect("handshake observed before timeout");
        assert!(result.is_err(), "test peer closes without completing TLS");
    }

    #[tokio::test]
    async fn connect_reports_transport_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("available local port");
        let address = listener.local_addr().expect("bound address");
        drop(listener);
        let config = ClientConfig::new("project", "instance")
            .expect("valid config")
            .with_emulator_host(address.to_string())
            .expect("valid emulator")
            .with_connect_timeout(Duration::from_millis(100))
            .expect("valid timeout");

        let error = connect(&config)
            .await
            .expect_err("closed port must refuse connection");

        assert!(matches!(error, Error::Transport { .. }));
        assert!(
            error
                .to_string()
                .starts_with("failed to connect to Bigtable:")
        );
    }
}
