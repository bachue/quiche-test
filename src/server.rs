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
    fs::read,
    io::ErrorKind as IOErrorKind,
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
    pin::Pin,
};
use tap::prelude::*;

const MAX_DATAGRAM_SIZE: usize = 1350;
const ONE_GB: u64 = 1 << 30;

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

type ClientMap = HashMap<ConnectionId<'static>, (SocketAddr, Client)>;

pub(super) fn start_server(socket: mio::net::UdpSocket) {
    start_server_inner(socket).expect("Failed to start http server");

    fn start_server_inner(mut socket: mio::net::UdpSocket) -> anyhow::Result<()> {
        let mut events = mio::Events::with_capacity(1024);

        let mut poll = mio::Poll::new().unwrap();
        poll.registry()
            .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

        let mut config = make_quiche_config()?;
        let h3_config = h3::Config::new()?;

        let conn_id_seed = Key::generate(HMAC_SHA256, &SystemRandom::new()).unwrap();
        let mut clients = ClientMap::new();

        events_loop(
            &mut clients,
            &mut events,
            &mut poll,
            &mut socket,
            &conn_id_seed,
            &mut config,
            &h3_config,
        )?;

        Ok(())
    }
}

fn events_loop(
    clients: &mut ClientMap,
    events: &mut mio::Events,
    poll: &mut mio::Poll,
    socket: &mut mio::net::UdpSocket,
    conn_id_seed: &Key,
    config: &mut quiche::Config,
    h3_config: &h3::Config,
) -> anyhow::Result<()> {
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    loop {
        let timeout = clients.values().filter_map(|(_, c)| c.conn.timeout()).min();

        poll.poll(events, timeout)?;

        'read: loop {
            if events.is_empty() {
                warn!("timed out");

                clients.values_mut().for_each(|(_, c)| c.conn.on_timeout());

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

                    panic!("recv() failed: {:?}", e);
                }
            };
            debug!("got {} bytes", len);

            let pkt_buf = &mut buf[..len];
            let hdr = match Header::from_slice(pkt_buf, MAX_CONN_ID_LEN) {
                Ok(v) => v,
                Err(e) => {
                    error!("Parsing packet header failed: {:?}", e);
                    continue 'read;
                }
            };

            debug!("got packet {:?}", hdr);

            let conn_id = hmac::sign(&conn_id_seed, &hdr.dcid);
            let conn_id = &conn_id.as_ref()[..MAX_CONN_ID_LEN];
            let conn_id = conn_id.to_vec().into();

            let (_, client) = if clients.contains_key(&hdr.dcid) || clients.contains_key(&conn_id) {
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
                        panic!("send() failed: {:?}", e);
                    }
                    continue 'read;
                }
                let mut scid = [0; MAX_CONN_ID_LEN];
                scid.copy_from_slice(&conn_id);
                let scid = ConnectionId::from_ref(&scid);
                let token = hdr.token.as_ref().unwrap();
                if token.is_empty() {
                    warn!("Doing stateless retry");

                    let new_token = mint_token(&hdr, src);
                    let len = quiche::retry(
                        &hdr.scid,
                        &hdr.dcid,
                        &scid,
                        &new_token,
                        hdr.version,
                        &mut out,
                    )?;
                    let out = &out[..len];

                    if let Err(e) = socket.send_to(out, src) {
                        if e.kind() == IOErrorKind::WouldBlock {
                            debug!("send() would block");
                            break;
                        }

                        panic!("send() failed: {:?}", e);
                    }
                    continue 'read;
                }
                let odcid = validate_token(src, token);

                if odcid.is_none() {
                    error!("Invalid address validation token");
                    continue 'read;
                }

                if scid.len() != hdr.dcid.len() {
                    error!("Invalid destination connection ID");
                    continue 'read;
                }

                let scid = hdr.dcid.clone();

                debug!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

                let conn = quiche::accept(&scid, odcid.as_ref(), config)?;

                let client = Client {
                    conn,
                    http3_conn: None,
                    partial_requests: Default::default(),
                    partial_responses: Default::default(),
                };

                clients.insert(scid.to_owned(), (src, client));
                clients.get_mut(&scid).unwrap()
            };

            let read = match client.conn.recv(pkt_buf) {
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
                        Ok((stream_id, quiche::h3::Event::Headers { list, has_body })) => {
                            // client
                            //     .partial_requests
                            //     .entry(stream_id)
                            //     .and_modify(|request| {
                            //         request.headers.extend_from_slice(&list);
                            //     })
                            //     .or_insert(PartialRequest {
                            //         headers: list,
                            //         body: Default::default(),
                            //     });
                            if !has_body {
                                handle_request(client, stream_id, &list, "/etc");
                            } else {
                                unimplemented!();
                            }
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

                        Ok((_stream_id, quiche::h3::Event::Finished)) => (),

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
        for (peer, client) in clients.values_mut() {
            loop {
                let write = match client.conn.send(&mut out) {
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

                if let Err(e) = socket.send_to(&out[..write], *peer) {
                    if e.kind() == IOErrorKind::WouldBlock {
                        debug!("send() would block");
                        break;
                    }

                    panic!("send() failed: {:?}", e);
                }

                debug!("{} written {} bytes", client.conn.trace_id(), write);
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
}

const TOKEN_PREFIX: &[u8] = b"quiche";

fn mint_token(hdr: &quiche::Header, src: SocketAddr) -> Vec<u8> {
    let mut token = Vec::new();

    token.extend_from_slice(TOKEN_PREFIX);

    let addr = match src.ip() {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    };

    token.extend_from_slice(&addr);
    token.extend_from_slice(&hdr.dcid);

    token
}

fn validate_token<'a>(src: SocketAddr, token: &'a [u8]) -> Option<quiche::ConnectionId<'a>> {
    const TOKEN_PREFIX_LEN: usize = TOKEN_PREFIX.len();

    if token.len() < TOKEN_PREFIX_LEN {
        return None;
    }

    if &token[..TOKEN_PREFIX_LEN] != TOKEN_PREFIX {
        return None;
    }

    let token = &token[TOKEN_PREFIX_LEN..];

    let addr = match src.ip() {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    };

    if token.len() < addr.len() || &token[..addr.len()] != addr.as_slice() {
        return None;
    }

    let token = &token[addr.len()..];
    Some(ConnectionId::from_ref(&token[..]))
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

fn handle_request(client: &mut Client, stream_id: u64, headers: &[h3::Header], root: &str) {
    let conn = &mut client.conn;
    let http3_conn = &mut client.http3_conn.as_mut().unwrap();

    info!(
        "{} got request {:?} on stream id {}",
        conn.trace_id(),
        headers,
        stream_id
    );

    conn.stream_shutdown(stream_id, quiche::Shutdown::Read, 0)
        .unwrap();

    let (headers, body) = build_response(root, headers);

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

fn build_response(
    root: &str,
    request: &[quiche::h3::Header],
) -> (Vec<quiche::h3::Header>, Vec<u8>) {
    let mut file_path = PathBuf::from(root);
    let mut path = Path::new("");
    let mut method = "";

    for hdr in request {
        match hdr.name() {
            ":path" => {
                path = Path::new(hdr.value());
            }
            ":method" => {
                method = hdr.value();
            }
            _ => (),
        }
    }
    let (status, body) = match method {
        "GET" => {
            for c in path.components() {
                if let Component::Normal(v) = c {
                    file_path.push(v)
                }
            }

            match read(&file_path) {
                Ok(data) => (200, data),
                Err(_) => (404, b"Not Found!".to_vec()),
            }
        }

        _ => (405, Vec::new()),
    };

    let headers = vec![
        h3::Header::new(":status", &status.to_string()),
        h3::Header::new("server", "quiche"),
        h3::Header::new("content-length", &body.len().to_string()),
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
