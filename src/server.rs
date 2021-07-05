use anyhow::bail;
use dashmap::DashMap;
use log::{debug, error, info, warn};
use quiche::{
    h3::{self, NameValue},
    Connection, ConnectionId, Header, MAX_CONN_ID_LEN,
};
use ring::{
    hmac::{self, Key, HMAC_SHA256},
    rand::SystemRandom,
};
use std::{
    collections::HashMap,
    io::{ErrorKind as IOErrorKind, Read},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};
use tap::prelude::*;

use crate::{MAX_DATAGRAM_SIZE, ONE_GB};

struct PartialRequest {
    headers: Vec<h3::Header>,
    body: Vec<u8>,
}

struct PartialResponse {
    headers: Option<Vec<h3::Header>>,
    body: Vec<u8>,
    written: usize,
}

struct Client {
    conn: Pin<Box<Connection>>,
    http3_conn: Option<h3::Connection>,
    partial_requests: HashMap<u64, PartialRequest>,
    partial_responses: HashMap<u64, PartialResponse>,
}

type ClientMap = DashMap<ConnectionId<'static>, (SocketAddr, Client)>;

const SERVER_TOKEN: mio::Token = mio::Token(0);
const RECEIVER_TOKEN: mio::Token = mio::Token(1);
static CLIENTS_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(super) fn start_server(
    socket: mio::net::UdpSocket,
    tasks_number: Arc<AtomicUsize>,
    receiver: mio::unix::pipe::Receiver,
) {
    start_server_inner(socket, tasks_number, receiver).expect("Failed to start http server");

    fn start_server_inner(
        mut socket: mio::net::UdpSocket,
        tasks_number: Arc<AtomicUsize>,
        mut receiver: mio::unix::pipe::Receiver,
    ) -> anyhow::Result<()> {
        let mut poll = mio::Poll::new().unwrap();
        poll.registry()
            .register(&mut socket, SERVER_TOKEN, mio::Interest::READABLE)?;
        poll.registry()
            .register(&mut receiver, RECEIVER_TOKEN, mio::Interest::READABLE)?;

        events_loop(&mut poll, &mut socket, tasks_number, &mut receiver)?;

        Ok(())
    }
}

fn events_loop(
    poll: &mut mio::Poll,
    socket: &mut mio::net::UdpSocket,
    tasks_number: Arc<AtomicUsize>,
    receiver: &mut mio::unix::pipe::Receiver,
) -> anyhow::Result<()> {
    let mut events = mio::Events::with_capacity(1024);
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];
    let mut config = make_quiche_config()?;
    let h3_config = h3::Config::new()?;
    let conn_id_seed = Key::generate(HMAC_SHA256, &SystemRandom::new()).unwrap();
    let clients = Arc::new(ClientMap::new());

    'events: while tasks_number.load(Relaxed) > 0 {
        let timeout = clients
            .iter()
            .filter_map(|r| {
                let (_, c) = r.value();
                c.conn.timeout()
            })
            .min();

        poll.poll(&mut events, timeout)?;

        'read: loop {
            if tasks_number.load(Relaxed) == 0 {
                break 'events;
            }

            if events.is_empty() {
                warn!("timed out after {:?}", timeout);
                clients.iter_mut().for_each(|mut r| {
                    let (_, c) = r.value_mut();
                    c.conn.on_timeout()
                });
                break 'read;
            }

            if events
                .iter()
                .find(|event| event.token() == RECEIVER_TOKEN)
                .is_some()
            {
                let mut buf = Vec::with_capacity(4);
                receiver.read_to_end(&mut buf).ok();
                if tasks_number.load(Relaxed) == 0 {
                    break 'events;
                }
            }
            if events
                .iter()
                .find(|event| event.token() == SERVER_TOKEN)
                .is_none()
            {
                break 'read;
            }

            let (len, src) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    // There are no more UDP packets to read, so end the read
                    // loop.
                    if e.kind() == IOErrorKind::WouldBlock {
                        debug!("recv() would block");
                        break 'read;
                    }

                    bail!("recv() failed: {:?}", e);
                }
            };
            debug!("got {} bytes from {:?}", len, src);

            let pkt_buf = &mut buf[..len];
            let hdr = match Header::from_slice(pkt_buf, MAX_CONN_ID_LEN) {
                Ok(v) => v,
                Err(e) => {
                    error!("Parsing packet header failed: {:?}", e);
                    continue 'read;
                }
            };

            debug!("got packet {:?}", hdr);

            let conn_id = {
                let conn_id = hmac::sign(&conn_id_seed, &hdr.dcid);
                let conn_id = &conn_id.as_ref()[..MAX_CONN_ID_LEN];
                conn_id.to_vec().into()
            };

            let mut r = if clients.contains_key(&hdr.dcid) || clients.contains_key(&conn_id) {
                match clients.get_mut(&hdr.dcid) {
                    Some(v) => v,
                    None => clients.get_mut(&conn_id).unwrap(),
                }
            } else {
                if hdr.ty != quiche::Type::Initial {
                    error!("Packet is not Initial");
                    continue 'read;
                }
                if !quiche::version_is_supported(hdr.version) {
                    warn!("Doing version negotiation");

                    let len = quiche::negotiate_version(&hdr.scid, &hdr.dcid, &mut out).unwrap();
                    let out = &out[..len];

                    if let Err(e) = socket.send_to(out, src) {
                        if e.kind() == IOErrorKind::WouldBlock {
                            debug!("send() would block");
                            break;
                        }
                        bail!("send() failed: {:?}", e);
                    }
                    continue 'read;
                }
                let mut scid = vec![0; MAX_CONN_ID_LEN];
                scid.copy_from_slice(&conn_id);
                let scid = ConnectionId::from_vec(scid);
                debug!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

                let conn = quiche::accept(&scid, None, src, &mut config)?;

                let client = Client {
                    conn,
                    http3_conn: None,
                    partial_requests: Default::default(),
                    partial_responses: Default::default(),
                };

                if clients.insert(scid.to_owned(), (src, client)).is_none() {
                    CLIENTS_COUNTER.fetch_add(1, Relaxed);
                }
                clients.get_mut(&scid).unwrap()
            };
            let (_, client) = &mut r.value_mut();

            let read = match client.conn.recv(pkt_buf, quiche::RecvInfo { from: src }) {
                Ok(v) => v,
                Err(e) => {
                    error!("{} recv failed: {:?}", client.conn.trace_id(), e);
                    continue 'read;
                }
            };

            debug!("{} processed {} bytes", client.conn.trace_id(), read);

            if (client.conn.is_in_early_data() || client.conn.is_established())
                && client.http3_conn.is_none()
            {
                debug!(
                    "{} QUIC handshake completed, now trying HTTP/3",
                    client.conn.trace_id()
                );

                let h3_conn = match h3::Connection::with_transport(&mut client.conn, &h3_config) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("failed to create HTTP/3 connection: {}", e);
                        continue 'read;
                    }
                };

                client.http3_conn = Some(h3_conn);
            }

            if client.http3_conn.is_some() {
                // Handle writable streams.
                for stream_id in client.conn.writable() {
                    handle_writable(client, stream_id);
                }

                // Process HTTP/3 events.
                loop {
                    let http3_conn = client.http3_conn.as_mut().unwrap();
                    match http3_conn
                        .poll(&mut client.conn)
                        .tap_ok(|(stream_id, event)| {
                            debug!("Event received for stream {}: {:?}", stream_id, event)
                        }) {
                        Ok((stream_id, quiche::h3::Event::Headers { list, .. })) => {
                            client
                                .partial_requests
                                .entry(stream_id)
                                .or_insert(PartialRequest {
                                    headers: list,
                                    body: Default::default(),
                                });
                        }

                        Ok((stream_id, quiche::h3::Event::Data)) => {
                            let mut data_buf = [0u8; 65536];
                            loop {
                                match http3_conn.recv_body(
                                    &mut client.conn,
                                    stream_id,
                                    data_buf.as_mut(),
                                ) {
                                    Ok(size) => {
                                        if let Some(request) =
                                            client.partial_requests.get_mut(&stream_id)
                                        {
                                            request.body.extend_from_slice(&data_buf[..size]);
                                        }
                                    }
                                    Err(h3::Error::Done) => {
                                        break;
                                    }
                                    Err(err) => {
                                        error!(
                                            "{} HTTP/3 Receive Data Error: {}",
                                            client.conn.trace_id(),
                                            err
                                        );
                                    }
                                }
                            }
                        }

                        Ok((stream_id, quiche::h3::Event::Finished)) => {
                            handle_request(client, stream_id);
                        }

                        Ok((_flow_id, quiche::h3::Event::Datagram)) => (),

                        Ok((_goaway_id, quiche::h3::Event::GoAway)) => (),

                        Err(quiche::h3::Error::Done) => {
                            break;
                        }

                        Err(e) => {
                            error!("{} HTTP/3 error {:?}", client.conn.trace_id(), e);

                            break;
                        }
                    }
                }
            }
        }
        for mut r in clients.iter_mut() {
            let (_, client) = r.value_mut();
            loop {
                let (written, send_info) = match client.conn.send(&mut out) {
                    Ok(v) => v,

                    Err(quiche::Error::Done) => {
                        debug!("{} done writing", client.conn.trace_id());
                        break;
                    }

                    Err(e) => {
                        error!("{} send failed: {:?}", client.conn.trace_id(), e);

                        client.conn.close(false, 0x1, b"fail").ok();
                        break;
                    }
                };

                if let Err(e) = socket.send_to(&out[..written], send_info.to) {
                    if e.kind() == IOErrorKind::WouldBlock {
                        debug!("send() would block");
                        break;
                    }

                    bail!("send() failed: {:?}", e);
                }

                debug!(
                    "{} written {} bytes to {:?}",
                    client.conn.trace_id(),
                    written,
                    send_info.to,
                );
            }
        }

        clients.retain(|_, (_, ref mut c)| {
            debug!("Collecting garbage");

            if c.conn.is_closed() {
                info!(
                    "{} connection collected {:?}",
                    c.conn.trace_id(),
                    c.conn.stats()
                );
                false
            } else {
                true
            }
        });
    }

    info!("Clients count: {}", CLIENTS_COUNTER.load(Relaxed));
    Ok(())
}

fn make_quiche_config() -> anyhow::Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

    config.load_cert_chain_from_pem_file("cert.crt")?;
    config.load_priv_key_from_pem_file("cert.key")?;
    config.set_application_protos(h3::APPLICATION_PROTOCOL)?;
    config.set_max_idle_timeout(30_000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(ONE_GB);
    config.set_initial_max_stream_data_bidi_local(ONE_GB);
    config.set_initial_max_stream_data_bidi_remote(ONE_GB);
    config.set_initial_max_stream_data_uni(ONE_GB);
    config.set_initial_max_streams_bidi(1000);
    config.set_initial_max_streams_uni(1000);
    config.set_disable_active_migration(true);
    config.enable_early_data();

    Ok(config)
}

fn handle_request(client: &mut Client, stream_id: u64) {
    let conn = &mut client.conn;
    let http3_conn = &mut client.http3_conn.as_mut().unwrap();

    if let Some(request) = client.partial_requests.remove(&stream_id) {
        info!(
            "{} got request body length {} on stream id {}",
            conn.trace_id(),
            request.body.len(),
            stream_id
        );

        conn.stream_shutdown(stream_id, quiche::Shutdown::Read, 0)
            .unwrap();

        let (headers, body) = build_response(&request);

        match http3_conn.send_response(conn, stream_id, &headers, false) {
            Ok(v) => v,
            Err(h3::Error::StreamBlocked) => {
                let response = PartialResponse {
                    headers: Some(headers),
                    body,
                    written: 0,
                };

                client.partial_responses.insert(stream_id, response);
                return;
            }
            Err(e) => {
                error!("{} stream send failed {:?}", conn.trace_id(), e);
                return;
            }
        }

        let written = match http3_conn.send_body(conn, stream_id, &body, true) {
            Ok(v) => v,
            Err(quiche::h3::Error::Done) => 0,
            Err(e) => {
                error!("{} stream send failed {:?}", conn.trace_id(), e);
                return;
            }
        };

        if written < body.len() {
            let response = PartialResponse {
                headers: None,
                body,
                written,
            };

            client.partial_responses.insert(stream_id, response);
        }
    }
}

fn build_response(request: &PartialRequest) -> (Vec<quiche::h3::Header>, Vec<u8>) {
    let mut method: &[u8] = &[];

    for hdr in request.headers.iter() {
        match hdr.name() {
            b":path" => {
                method = hdr.value();
            }
            _ => (),
        }
    }
    let (status, body) = match method {
        b"POST" => (200, b"{\"key\":\"testkey\",\"hash\":\"testhash\"}".to_vec()),
        _ => (405, Vec::new()),
    };

    let headers = vec![
        h3::Header::new(b":status", status.to_string().as_bytes()),
        h3::Header::new(b"server", b"quiche"),
        h3::Header::new(b"content-length", body.len().to_string().as_bytes()),
    ];

    (headers, body)
}

fn handle_writable(client: &mut Client, stream_id: u64) {
    let conn = &mut client.conn;
    let http3_conn = &mut client.http3_conn.as_mut().unwrap();

    debug!("{} stream {} is writable", conn.trace_id(), stream_id);

    if let Some(resp) = client.partial_responses.get_mut(&stream_id) {
        if let Some(headers) = resp.headers.take() {
            match http3_conn.send_response(conn, stream_id, &headers, false) {
                Ok(_) => (),
                Err(h3::Error::StreamBlocked) => {
                    return;
                }
                Err(err) => {
                    error!("{} stream send failed {:?}", conn.trace_id(), err);
                    return;
                }
            }
        }
        let body = &resp.body[resp.written..];
        let written = match http3_conn.send_body(conn, stream_id, body, true) {
            Ok(v) => v,
            Err(h3::Error::Done) => 0,
            Err(err) => {
                client.partial_responses.remove(&stream_id);
                error!("{} stream send failed {:?}", conn.trace_id(), err);
                return;
            }
        };
        resp.written += written;
        if resp.written >= resp.body.len() {
            client.partial_responses.remove(&stream_id);
        }
    }
}
