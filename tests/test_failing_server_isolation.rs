use tokio::net::UdpSocket;
use tracing_subscriber::prelude::*;

use lorawan_multiplexer_converter::{config, forwarder, listener};

/// A server that cannot be reached must not stop the servers configured after
/// it from receiving the uplink.
#[tokio::test]
async fn test() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let conf = config::Configuration {
        gwmp: config::Gwmp {
            inputs: vec![config::GwmpInput {
                bind: "0.0.0.0:1720".into(),
                ..Default::default()
            }],
            outputs: vec![
                // Port 0 is not connectable, so this output fails while setting
                // up its socket — every packet to it errors out.
                config::GwmpOutput {
                    server: "127.0.0.1:0".into(),
                    ..Default::default()
                },
                config::GwmpOutput {
                    server: "localhost:1721".into(),
                    ..Default::default()
                },
            ],
        },
        ..Default::default()
    };

    let (downlink_tx, uplink_rx, _uplink_tx) = listener::setup(&conf.gwmp.inputs).await.unwrap();
    forwarder::setup(downlink_tx, uplink_rx, conf.gwmp.outputs.clone())
        .await
        .unwrap();
    let mut buffer: [u8; 65535] = [0; 65535];

    // Healthy server socket.
    let server_sock = UdpSocket::bind("0.0.0.0:1721").await.unwrap();

    // Gateway socket.
    let gw_sock = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    gw_sock.connect("localhost:1720").await.unwrap();

    // Send PUSH_DATA (unconfirmed uplink with DevAddr 01020304).
    gw_sock
        .send(&[
            0x02, 0x01, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x7b, 0x22,
            0x72, 0x78, 0x70, 0x6b, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x64, 0x61, 0x74, 0x61, 0x22,
            0x3a, 0x22, 0x51, 0x41, 0x51, 0x44, 0x41, 0x67, 0x45, 0x3d, 0x22, 0x7d, 0x5d, 0x7d,
        ])
        .await
        .unwrap();

    // Expect PUSH_ACK.
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x01], &buffer[..size]);

    // Expect the registration PULL_DATA on the healthy server.
    let size = server_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(12, size);
    assert_eq!(0x02, buffer[3]);

    // Expect PUSH_DATA forwarded to the healthy server despite the first output
    // failing.
    let size = server_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(
        &[
            0x02, 0x01, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x7b, 0x22,
            0x72, 0x78, 0x70, 0x6b, 0x22, 0x3a, 0x5b, 0x7b, 0x22, 0x64, 0x61, 0x74, 0x61, 0x22,
            0x3a, 0x22, 0x51, 0x41, 0x51, 0x44, 0x41, 0x67, 0x45, 0x3d, 0x22, 0x7d, 0x5d, 0x7d,
        ],
        &buffer[..size]
    );
}
