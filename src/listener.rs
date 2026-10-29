use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow};
use chirpstack_api::gw;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{OnceCell, RwLock};
use tracing::{Instrument, debug, error, info, trace, warn};

use crate::basicstation;
use crate::config;
use crate::monitoring::{inc_gateway_udp_received_count, inc_gateway_udp_sent_count};
use crate::mqtt;
use crate::packets::{GatewayId, PacketType, PushData, TxAck, downlink_frame_item_to_pull_resp, get_random_token};
use crate::traits::PrintFullError;

static GATEWAYS: OnceCell<RwLock<HashMap<GatewayId, Gateway>>> = OnceCell::const_new();

/// Downlink IDs awaiting a TX_ACK, keyed by (gateway_id, token), with the time
/// the entry was stored so unacknowledged downlinks can be expired.
type PendingDownlinks = HashMap<(GatewayId, u16), (u32, Instant)>;

static PENDING_DOWNLINKS: OnceCell<RwLock<PendingDownlinks>> = OnceCell::const_new();

/// How long a downlink waits for its TX_ACK before the correlation entry is dropped.
/// A gateway that never acks would otherwise leak an entry per downlink forever.
const PENDING_DOWNLINK_TTL: Duration = Duration::from_secs(60);

/// A ChirpStack downlink frame with more than one item (e.g. RX1 and RX2),
/// sent to a UDP gateway one item at a time. When the gateway rejects an item
/// the next one is sent, as chirpstack-gateway-bridge does.
struct PendingFrame {
    frame: gw::DownlinkFrame,
    index: usize,
    statuses: Vec<gw::TxAckStatus>,
    stored_at: Instant,
}

/// Multi-item downlink frames awaiting a TX_ACK, keyed by (gateway_id, downlink_id).
static PENDING_FRAMES: OnceCell<RwLock<HashMap<(GatewayId, u32), PendingFrame>>> = OnceCell::const_new();

/// What to do with a TX_ACK for a downlink that came from a ChirpStack frame.
enum FrameAck {
    /// Not a tracked multi-item frame; report the gateway's TX_ACK as-is.
    Untracked,
    /// The item was rejected; send this PULL_RESP for the next item instead of acking.
    Retry(Vec<u8>),
    /// An item was accepted or none are left; report a status for every item.
    Done(Vec<gw::TxAckStatus>),
}

struct Gateway {
    addr: SocketAddr,
    socket: Arc<UdpSocket>,
    last_seen: SystemTime,
}

pub async fn setup(
    inputs: &[config::GwmpInput],
) -> Result<(
    UnboundedSender<(GatewayId, Vec<u8>, Option<u32>)>,  // downlink_tx (gateway_id, data, downlink_id)
    UnboundedReceiver<(GatewayId, Vec<u8>, String, Option<String>, String)>, // uplink_rx (gateway_id, data, region)
    UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>, String)>,  // uplink_tx (for MQTT-in)
)> {
    let (uplink_tx, uplink_rx) = unbounded_channel::<(GatewayId, Vec<u8>, String, Option<String>, String)>();
    let (downlink_tx, downlink_rx) = unbounded_channel::<(GatewayId, Vec<u8>, Option<u32>)>();

    for input in inputs {
        if input.name.is_empty() {
            info!(bind = %input.bind, topic_prefix = %input.topic_prefix, "Setting up listener");
        } else {
            info!(bind = %input.bind, topic_prefix = %input.topic_prefix, name = %input.name, "Setting up listener");
        }

        let sock = UdpSocket::bind(&input.bind).await.context("Bind socket")?;
        let sock = Arc::new(sock);

        let topic_prefix = input.topic_prefix.clone();
        let input_name = input.name.clone();
        tokio::spawn(handle_uplink(sock.clone(), uplink_tx.clone(), topic_prefix, input_name));
    }

    tokio::spawn(handle_downlink(downlink_rx));
    tokio::spawn(cleanup_gateways());

    Ok((downlink_tx, uplink_rx, uplink_tx))
}

async fn handle_uplink(socket: Arc<UdpSocket>, uplink_tx: UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>, String)>, topic_prefix: String, input_name: String) {
    let mut buffer: [u8; 65535] = [0; 65535];
    loop {
        let (size, addr) = match socket.recv_from(&mut buffer).await {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Receive error");
                continue;
            }
        };

        if size < 4 {
            warn!(addr = %addr, received_bytes = size, "At least 4 bytes are expected");
            continue;
        }

        if let Err(e) = handle_uplink_packet(&socket, &uplink_tx, addr, &buffer[..size], &topic_prefix, &input_name)
            .instrument(tracing::info_span!("", addr = %addr))
            .await
        {
            error!(error = %e.full(), "Handle uplink packet error");
        }
    }
}

async fn handle_uplink_packet(
    socket: &Arc<UdpSocket>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>, String)>,
    addr: SocketAddr,
    data: &[u8],
    topic_prefix: &str,
    input_name: &str,
) -> Result<()> {
    let packet_type = PacketType::try_from(data)?;
    let gateway_id = GatewayId::try_from(data)?;
    let _token = get_random_token(data)?;

    // For PushData, extract and log DevAddr info from rxpk entries
    let frame_info = if packet_type == PacketType::PushData {
        PushData::from_slice(data)
            .ok()
            .map(|pd| {
                pd.payload.rxpk.iter()
                    .map(|rxpk| rxpk.get_frame_info())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .filter(|s| !s.is_empty())
    } else {
        None
    };

    // Prefix the message with the input name when one is configured, e.g.
    // "[openlns] Received UDP uplink". Unnamed inputs get no prefix.
    let name_prefix = if input_name.is_empty() {
        String::new()
    } else {
        format!("[{}] ", input_name)
    };
    if let Some(ref frames) = frame_info {
        info!(
            gateway_id = %gateway_id,
            frames = %frames,
            "{}Received UDP uplink", name_prefix,
        );
    } else {
        info!(
            packet_type = %packet_type,
            gateway_id = %gateway_id,
            "{}Received UDP packet", name_prefix,
        );
    }

    inc_gateway_udp_received_count(gateway_id, packet_type).await;

    match packet_type {
        PacketType::PushData => handle_push_data(socket, uplink_tx, addr, gateway_id, data, topic_prefix, input_name).await?,
        PacketType::PullData => {
            set_gateway(gateway_id, addr, socket.clone()).await?;
            handle_pull_data(socket, uplink_tx, addr, gateway_id, data, topic_prefix, input_name).await?;
        }
        PacketType::TxAck => handle_tx_ack(uplink_tx, gateway_id, data, topic_prefix, input_name).await?,
        _ => warn!(packet_type = %packet_type, "Unexpected packet-type"),
    }

    Ok(())
}

async fn handle_downlink(
    mut downlink_rx: UnboundedReceiver<(GatewayId, Vec<u8>, Option<u32>)>,
) {
    while let Some((gateway_id, data, downlink_id)) = downlink_rx.recv().await {
        if let Err(e) = handle_downlink_packet(gateway_id, &data, downlink_id).await {
            error!(error = %e.full(), "Handle downlink packet error");
        }
    }
}

async fn handle_downlink_packet(
    gateway_id: GatewayId,
    data: &[u8],
    downlink_id: Option<u32>,
) -> Result<()> {
    let packet_type = PacketType::try_from(data)?;

    // Check if this is a Basic Station gateway first
    if basicstation::is_bs_gateway(gateway_id).await {
        basicstation::send_downlink_frame(gateway_id, data).await?;
        return Ok(());
    }

    // Check if this is an MQTT gateway (includes virtual relay gateways)
    if mqtt::is_mqtt_gateway(gateway_id).await {
        // Route via MQTT (send_downlink_frame handles relay→border resolution)
        mqtt::send_downlink_frame(gateway_id, data).await?;
        return Ok(());
    }

    // Fallback: check if this is a virtual relay gateway whose border gateway is an MQTT gateway
    if let Some(border_gw) = mqtt::resolve_relay_gateway(gateway_id).await {
        if mqtt::is_mqtt_gateway(border_gw).await {
            info!(
                virtual_gateway = %gateway_id,
                border_gateway = %border_gw,
                "Routing relay downlink to border gateway (fallback)"
            );
            mqtt::send_downlink_frame(gateway_id, data).await?;
            return Ok(());
        }
    }

    // For PULL_RESP, extract the token and store the mapping to the real downlink_id
    if packet_type == PacketType::PullResp && data.len() >= 4 {
        let token = u16::from_le_bytes([data[1], data[2]]);
        // Use the provided downlink_id if available, otherwise fall back to token
        let real_downlink_id = downlink_id.unwrap_or(token as u32);
        store_pending_downlink(gateway_id, token, real_downlink_id).await;
    }

    // Route via UDP — look up the gateway's socket and address
    let (addr, socket) = get_gateway_socket(gateway_id).await?;
    let span = tracing::info_span!("", addr = %addr);

    async move {
        info!(packet_type = %packet_type, gateway_id = %gateway_id, "Sending UDP packet");

        socket
            .send_to(data, addr)
            .await
            .context("Socket send")
            .map(|_| ())
    }
    .instrument(span)
    .await?;

    inc_gateway_udp_sent_count(gateway_id, packet_type).await;

    Ok(())
}

async fn handle_push_data(
    socket: &Arc<UdpSocket>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>, String)>,
    addr: SocketAddr,
    gateway_id: GatewayId,
    data: &[u8],
    topic_prefix: &str,
    input_name: &str,
) -> Result<()> {
    if data.len() < 12 {
        return Err(anyhow!("At least 12 bytes are expected"));
    }

    info!(packet_type = %PacketType::PushAck, "Sending UDP packet");

    let b: [u8; 4] = [data[0], data[1], data[2], PacketType::PushAck.into()];
    socket.send_to(&b, addr).await.context("Socket send")?;
    inc_gateway_udp_sent_count(gateway_id, PacketType::PushAck).await;

    debug!("Sending received data to uplink channel");
    uplink_tx
        .send((gateway_id, data.to_vec(), topic_prefix.to_string(), None, input_name.to_string()))
        .context("Uplink channel send")?;

    Ok(())
}

async fn handle_tx_ack(
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>, String)>,
    gateway_id: GatewayId,
    data: &[u8],
    topic_prefix: &str,
    input_name: &str,
) -> Result<()> {
    // Forward to UDP servers via uplink channel
    uplink_tx
        .send((gateway_id, data.to_vec(), topic_prefix.to_string(), None, input_name.to_string()))
        .context("Uplink channel send")?;

    // Forward to MQTT backends if enabled
    let mqtt_enabled = mqtt::is_enabled().await;
    let is_mqtt_gw = mqtt::is_mqtt_gateway(gateway_id).await;

    debug!(
        gateway_id = %gateway_id,
        mqtt_enabled = mqtt_enabled,
        is_mqtt_gateway = is_mqtt_gw,
        "TX_ACK received, checking MQTT forwarding"
    );

    if mqtt_enabled && !is_mqtt_gw {
        match TxAck::from_slice(data) {
            Ok(tx_ack) => {
                let token = tx_ack.random_token;
                // Look up the downlink_id from our pending map
                let downlink_id = get_pending_downlink(gateway_id, token).await.unwrap_or(token as u32);
                let ack = match advance_downlink_frame(gateway_id, downlink_id, tx_ack.status()).await {
                    FrameAck::Untracked => tx_ack.to_downlink_tx_ack(&gateway_id, downlink_id),
                    FrameAck::Retry(pull_resp) => {
                        // ChirpStack only hears about the downlink once the last
                        // attempt is acked, so the retry's own TX_ACK reports it.
                        return handle_downlink_packet(gateway_id, &pull_resp, Some(downlink_id)).await;
                    }
                    FrameAck::Done(statuses) => gw::DownlinkTxAck {
                        gateway_id: gateway_id.to_string(),
                        downlink_id,
                        items: statuses
                            .into_iter()
                            .map(|s| gw::DownlinkTxAckItem { status: s.into() })
                            .collect(),
                        downlink_id_legacy: Vec::new(),
                        gateway_id_legacy: Vec::new(),
                    },
                };

                info!(
                    gateway_id = %gateway_id,
                    downlink_id = downlink_id,
                    "Forwarding TX_ACK to MQTT"
                );

                if let Err(e) = mqtt::send_tx_ack(gateway_id, &ack, topic_prefix).await {
                    error!(error = %e, "MQTT send TX_ACK error");
                }
            }
            Err(e) => {
                warn!(gateway_id = %gateway_id, error = %e, "Failed to parse TX_ACK");
            }
        }
    } else {
        debug!(
            gateway_id = %gateway_id,
            mqtt_enabled = mqtt_enabled,
            is_mqtt_gateway = is_mqtt_gw,
            "TX_ACK not forwarded to MQTT (mqtt_enabled={}, is_mqtt_gateway={})",
            mqtt_enabled,
            is_mqtt_gw
        );
    }

    Ok(())
}

async fn handle_pull_data(
    socket: &Arc<UdpSocket>,
    uplink_tx: &UnboundedSender<(GatewayId, Vec<u8>, String, Option<String>, String)>,
    addr: SocketAddr,
    gateway_id: GatewayId,
    data: &[u8],
    topic_prefix: &str,
    input_name: &str,
) -> Result<()> {
    if data.len() < 12 {
        return Err(anyhow!("At least 12 bytes are expected"));
    }

    info!(packet_type = %PacketType::PullAck, "Sending UDP packet");

    let b: [u8; 4] = [data[0], data[1], data[2], PacketType::PullAck.into()];
    socket.send_to(&b, addr).await.context("Socket send")?;
    inc_gateway_udp_sent_count(gateway_id, PacketType::PullAck).await;

    uplink_tx
        .send((gateway_id, data.to_vec(), topic_prefix.to_string(), None, input_name.to_string()))
        .context("Uplink channel send")?;

    Ok(())
}

async fn set_gateway(gateway_id: GatewayId, addr: SocketAddr, socket: Arc<UdpSocket>) -> Result<()> {
    trace!(gateway_id = %gateway_id, addr = %addr, "Setting / updating Gateway ID to addr mapping");

    let gateways = GATEWAYS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;

    let mut gateways = gateways.write().await;
    let _ = gateways.insert(
        gateway_id,
        Gateway {
            addr,
            socket,
            last_seen: SystemTime::now(),
        },
    );

    Ok(())
}

async fn get_gateway_socket(gateway_id: GatewayId) -> Result<(SocketAddr, Arc<UdpSocket>)> {
    trace!(gateway_id = %gateway_id, "Getting addr and socket for Gateway ID");

    let gateways = GATEWAYS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;

    let gateways = gateways.read().await;
    gateways
        .get(&gateway_id)
        .map(|v| (v.addr, v.socket.clone()))
        .ok_or_else(|| anyhow!("Unknown Gateway ID: {}", gateway_id))
}

async fn cleanup_gateways() {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

        trace!("Cleaning up inactive Gateway ID to addr mappings");

        cleanup_pending_downlinks().await;

        let gateways = GATEWAYS
            .get_or_init(|| async { RwLock::new(HashMap::new()) })
            .await;
        let mut gateways = gateways.write().await;
        gateways.retain(|k, v| {
            if let Ok(duration) = SystemTime::now().duration_since(v.last_seen) {
                if duration < Duration::from_secs(60) {
                    true
                } else {
                    warn!(gateway_id = %k, addr = %v.addr, "Cleaning up inactive mapping");
                    false
                }
            } else {
                warn!(gateway_id = %k, addr = %v.addr, "Cleaning up inactive mapping");
                false
            }
        })
    }
}

/// Store a pending downlink ID for later TX_ACK handling.
async fn store_pending_downlink(gateway_id: GatewayId, token: u16, downlink_id: u32) {
    debug!(gateway_id = %gateway_id, token = token, downlink_id = downlink_id, "Storing pending downlink for TX_ACK correlation");
    let pending = PENDING_DOWNLINKS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let mut pending = pending.write().await;
    pending.insert((gateway_id, token), (downlink_id, Instant::now()));
}

/// Keep a ChirpStack downlink frame so its later items (RX2) can be tried if the
/// gateway rejects an earlier one. Single-item frames need no tracking.
pub async fn store_downlink_frame(gateway_id: GatewayId, frame: &gw::DownlinkFrame) {
    if frame.items.len() < 2 {
        return;
    }
    let frames = PENDING_FRAMES
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let mut frames = frames.write().await;
    frames.insert(
        (gateway_id, frame.downlink_id),
        PendingFrame {
            frame: frame.clone(),
            index: 0,
            statuses: vec![gw::TxAckStatus::Ignored; frame.items.len()],
            stored_at: Instant::now(),
        },
    );
}

/// Record the gateway's status for the frame's current item and decide whether
/// to try the next item or report the result.
async fn advance_downlink_frame(gateway_id: GatewayId, downlink_id: u32, status: gw::TxAckStatus) -> FrameAck {
    let frames = match PENDING_FRAMES.get() {
        Some(f) => f,
        None => return FrameAck::Untracked,
    };
    let mut frames = frames.write().await;
    let key = (gateway_id, downlink_id);
    let pending = match frames.get_mut(&key) {
        Some(p) => p,
        None => return FrameAck::Untracked,
    };

    pending.statuses[pending.index] = status;

    while status != gw::TxAckStatus::Ok && pending.index + 1 < pending.frame.items.len() {
        pending.index += 1;
        match downlink_frame_item_to_pull_resp(&pending.frame, pending.index) {
            Ok(pull_resp) => {
                info!(
                    gateway_id = %gateway_id,
                    downlink_id = downlink_id,
                    status = ?status,
                    item_index = pending.index,
                    "Downlink item rejected, trying next item"
                );
                return FrameAck::Retry(pull_resp);
            }
            Err(e) => {
                warn!(gateway_id = %gateway_id, downlink_id = downlink_id, item_index = pending.index, error = %e, "Skipping unconvertible downlink item");
                pending.statuses[pending.index] = gw::TxAckStatus::InternalError;
            }
        }
    }

    frames.remove(&key).map(|p| FrameAck::Done(p.statuses)).unwrap_or(FrameAck::Untracked)
}

/// Drop downlink correlation entries that never received a TX_ACK.
async fn cleanup_pending_downlinks() {
    if let Some(frames) = PENDING_FRAMES.get() {
        frames.write().await.retain(|_, p| p.stored_at.elapsed() < PENDING_DOWNLINK_TTL);
    }

    let pending = match PENDING_DOWNLINKS.get() {
        Some(p) => p,
        None => return,
    };
    let mut pending = pending.write().await;
    let before = pending.len();
    pending.retain(|_, (_, stored_at)| stored_at.elapsed() < PENDING_DOWNLINK_TTL);
    let removed = before - pending.len();
    if removed > 0 {
        debug!(
            removed = removed,
            remaining = pending.len(),
            "Cleaned up unacknowledged downlink correlations"
        );
    }
}

/// Get and remove a pending downlink ID.
async fn get_pending_downlink(gateway_id: GatewayId, token: u16) -> Option<u32> {
    let pending = PENDING_DOWNLINKS
        .get_or_init(|| async { RwLock::new(HashMap::new()) })
        .await;
    let mut pending = pending.write().await;
    let result = pending.remove(&(gateway_id, token)).map(|(id, _)| id);
    debug!(gateway_id = %gateway_id, token = token, downlink_id = ?result, "Retrieved pending downlink for TX_ACK");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // RX1 at 926.9 MHz +5s, RX2 at 923.3 MHz +6s, as ChirpStack sends a join-accept.
    fn join_accept_frame(downlink_id: u32) -> gw::DownlinkFrame {
        let item = |frequency: u32, seconds: i64| gw::DownlinkFrameItem {
            phy_payload: vec![0x20, 0x01],
            tx_info: Some(gw::DownlinkTxInfo {
                frequency,
                power: 27,
                timing: Some(gw::Timing {
                    parameters: Some(gw::timing::Parameters::Delay(gw::DelayTimingInfo {
                        delay: Some(chirpstack_api::pbjson_types::Duration { seconds, nanos: 0 }),
                    })),
                }),
                context: 1_000_000u32.to_be_bytes().to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        };
        gw::DownlinkFrame {
            downlink_id,
            items: vec![item(926_900_000, 5), item(923_300_000, 6)],
            ..Default::default()
        }
    }

    fn txpk(pull_resp: &[u8]) -> serde_json::Value {
        serde_json::from_slice::<serde_json::Value>(&pull_resp[4..]).unwrap()["txpk"].clone()
    }

    #[tokio::test]
    async fn test_rejected_rx1_retries_rx2() {
        let gateway_id = GatewayId::from_hex("0000000000000001").unwrap();
        store_downlink_frame(gateway_id, &join_accept_frame(10)).await;

        let pull_resp = match advance_downlink_frame(gateway_id, 10, gw::TxAckStatus::TooEarly).await {
            FrameAck::Retry(b) => b,
            _ => panic!("expected RX2 retry"),
        };
        let txpk = txpk(&pull_resp);
        assert_eq!(txpk["freq"], serde_json::json!(923.3));
        assert_eq!(txpk["tmst"], serde_json::json!(7_000_000u32));

        match advance_downlink_frame(gateway_id, 10, gw::TxAckStatus::Ok).await {
            FrameAck::Done(s) => assert_eq!(s, vec![gw::TxAckStatus::TooEarly, gw::TxAckStatus::Ok]),
            _ => panic!("expected done"),
        }
        // The frame is forgotten once acked.
        assert!(matches!(advance_downlink_frame(gateway_id, 10, gw::TxAckStatus::Ok).await, FrameAck::Untracked));
    }

    #[tokio::test]
    async fn test_accepted_rx1_ignores_rx2() {
        let gateway_id = GatewayId::from_hex("0000000000000002").unwrap();
        store_downlink_frame(gateway_id, &join_accept_frame(20)).await;

        match advance_downlink_frame(gateway_id, 20, gw::TxAckStatus::Ok).await {
            FrameAck::Done(s) => assert_eq!(s, vec![gw::TxAckStatus::Ok, gw::TxAckStatus::Ignored]),
            _ => panic!("expected done"),
        }
    }

    #[tokio::test]
    async fn test_all_items_rejected() {
        let gateway_id = GatewayId::from_hex("0000000000000003").unwrap();
        store_downlink_frame(gateway_id, &join_accept_frame(30)).await;

        assert!(matches!(advance_downlink_frame(gateway_id, 30, gw::TxAckStatus::TooEarly).await, FrameAck::Retry(_)));
        match advance_downlink_frame(gateway_id, 30, gw::TxAckStatus::TooLate).await {
            FrameAck::Done(s) => assert_eq!(s, vec![gw::TxAckStatus::TooEarly, gw::TxAckStatus::TooLate]),
            _ => panic!("expected done"),
        }
    }

    #[tokio::test]
    async fn test_single_item_frame_untracked() {
        let gateway_id = GatewayId::from_hex("0000000000000004").unwrap();
        let mut frame = join_accept_frame(40);
        frame.items.truncate(1);
        store_downlink_frame(gateway_id, &frame).await;

        assert!(matches!(advance_downlink_frame(gateway_id, 40, gw::TxAckStatus::TooEarly).await, FrameAck::Untracked));
    }
}
