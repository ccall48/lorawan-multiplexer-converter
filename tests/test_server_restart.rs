use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};
use tracing_subscriber::prelude::*;

use lorawan_multiplexer_converter::{config, forwarder, listener};

// Tests that downlinks keep working after the server was unreachable for a
// moment (e.g. a restarting LNS container), which results in an ICMP port
// unreachable being reported on the forwarder socket.
#[tokio::test]
async fn test() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let conf = config::Configuration {
        gwmp: config::Gwmp {
            inputs: vec![config::GwmpInput {
                bind: "127.0.0.1:1720".into(),
                ..Default::default()
            }],
            outputs: vec![config::GwmpOutput {
                server: "127.0.0.1:1721".into(),
                ..Default::default()
            }],
        },
        ..Default::default()
    };

    let (downlink_tx, uplink_rx, _uplink_tx) = listener::setup(&conf.gwmp.inputs).await.unwrap();
    forwarder::setup(downlink_tx, uplink_rx, conf.gwmp.outputs.clone())
        .await
        .unwrap();
    let mut buffer: [u8; 65535] = [0; 65535];

    let pull_data = [
        0x02, 0x01, 0x02, 0x02, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    ];
    let pull_resp = [0x02, 0x01, 0x02, 0x03, 0x7b, 0x7d];

    // Server socket.
    let server_sock = UdpSocket::bind("127.0.0.1:1721").await.unwrap();

    // Gateway socket.
    let gw_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    gw_sock.connect("127.0.0.1:1720").await.unwrap();

    // Send PULL_DATA, expect PULL_ACK at gateway and PULL_DATA at server.
    gw_sock.send(&pull_data).await.unwrap();
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x04], &buffer[..size]);
    let (size, _) = server_sock.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&pull_data, &buffer[..size]);

    // Server goes away.
    drop(server_sock);

    // Send PULL_DATA while the server is down, this triggers an ICMP port
    // unreachable on the forwarder socket.
    gw_sock.send(&pull_data).await.unwrap();
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x04], &buffer[..size]);
    sleep(Duration::from_millis(100)).await;

    // Server comes back.
    let server_sock = UdpSocket::bind("127.0.0.1:1721").await.unwrap();

    // Send PULL_DATA, expect it at the server.
    gw_sock.send(&pull_data).await.unwrap();
    let size = gw_sock.recv(&mut buffer).await.unwrap();
    assert_eq!(&[0x02, 0x01, 0x02, 0x04], &buffer[..size]);
    let (size, addr) = server_sock.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&pull_data, &buffer[..size]);

    // Send PULL_RESP from server, expect it at the gateway.
    server_sock.send_to(&pull_resp, addr).await.unwrap();
    let size = timeout(Duration::from_secs(1), gw_sock.recv(&mut buffer))
        .await
        .expect("PULL_RESP was not forwarded to gateway")
        .unwrap();
    assert_eq!(&pull_resp, &buffer[..size]);
}
