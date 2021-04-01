use std::{
    io::ErrorKind as IOErrorKind,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{atomic::AtomicUsize, Arc, Mutex},
    time::Instant,
};

use crate::{tasks_number::TasksNumberGuard, MAX_DATAGRAM_SIZE, ONE_GB};
use anyhow::{bail, Context};
use log::{debug, error, info};
use quiche::h3;
use rayon::ThreadPoolBuilder;
use ring::rand::{SecureRandom, SystemRandom};
const TASK_COUNT: usize = 10000;
const WORKER_COUNT: usize = 10;

pub(super) fn new_tasks_number() -> Arc<AtomicUsize> {
    return Arc::new(AtomicUsize::new(TASK_COUNT));
}

pub(super) fn start_clients(
    server_address: SocketAddr,
    tasks_number: Arc<AtomicUsize>,
    sender: mio::unix::pipe::Sender,
) {
    start_clients_inner(server_address, tasks_number, sender)
        .expect("Failed to start http clients");

    fn start_clients_inner(
        server_address: SocketAddr,
        tasks_number: Arc<AtomicUsize>,
        sender: mio::unix::pipe::Sender,
    ) -> anyhow::Result<()> {
        let bind_addr = match server_address {
            SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            SocketAddr::V6(_) => {
                SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)), 0)
            }
        };
        let sender = Arc::new(Mutex::new(sender));

        ThreadPoolBuilder::new()
            .num_threads(WORKER_COUNT)
            .thread_name(|i| format!("quiche client worker - {}", i))
            .build()?
            .scope(|s| {
                for _ in 0..TASK_COUNT {
                    let tasks_number = tasks_number.to_owned();
                    let sender = sender.to_owned();
                    s.spawn(move |_| {
                        let _tasks_number_guard = TasksNumberGuard::new(tasks_number, sender);
                        start_client_worker(bind_addr, server_address)
                            .expect("Client worker failed")
                    });
                }
            });

        Ok(())
    }
}
fn start_client_worker(bind_addr: SocketAddr, server_address: SocketAddr) -> anyhow::Result<()> {
    let mut socket = mio::net::UdpSocket::bind(bind_addr)?;
    socket.connect(server_address)?;

    let mut poll = mio::Poll::new().unwrap();
    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

    let mut config = make_quiche_config().unwrap();
    let mut http3_conn = None;
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    SystemRandom::new().fill(&mut scid[..]).unwrap();
    let scid = quiche::ConnectionId::from_ref(&scid);
    let mut conn = quiche::connect(Some("localhost"), &scid, &mut config).unwrap();
    info!(
        "connecting to {:} from {:} with scid {}",
        server_address,
        socket.local_addr().unwrap(),
        hex::encode(&scid)
    );

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];
    let written = conn.send(&mut out).context("initial send failed")?;

    while let Err(err) = socket.send(&out[..written]) {
        if err.kind() == IOErrorKind::WouldBlock {
            debug!("send() would block");
            continue;
        }

        bail!("send() failed: {:?}", err);
    }
    debug!("written {}", written);

    let h3_config = quiche::h3::Config::new().unwrap();
    let request_headers = vec![
        h3::Header::new(":method", "GET"),
        h3::Header::new(":scheme", "https"),
        h3::Header::new(":authority", "localhost"),
        h3::Header::new(":path", "/services"),
        h3::Header::new("user-agent", "quiche"),
    ];
    let request_begin_at = Instant::now();
    let mut request_sent = false;
    let mut received_total = 0usize;

    let mut events = mio::Events::with_capacity(1024);

    loop {
        poll.poll(&mut events, conn.timeout())?;

        'read: loop {
            if events.is_empty() {
                debug!("timed out");

                conn.on_timeout();

                break 'read;
            }

            let len = match socket.recv(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == IOErrorKind::WouldBlock {
                        debug!("recv() would block");
                        break 'read;
                    }
                    bail!("recv() failed: {:?}", e);
                }
            };
            debug!("got {} bytes", len);

            let read = match conn.recv(&mut buf[..len]) {
                Ok(v) => v,
                Err(e) => {
                    error!("recv failed: {:?}", e);
                    continue 'read;
                }
            };

            debug!("processed {} bytes", read);
        }

        debug!("done reading");

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
            break;
        }

        if conn.is_established() && http3_conn.is_none() {
            http3_conn = Some(h3::Connection::with_transport(&mut conn, &h3_config)?);
        }

        if let Some(http3_conn) = &mut http3_conn {
            if !request_sent {
                info!("sending HTTP request {:?}", request_headers);

                http3_conn
                    .send_request(&mut conn, &request_headers, true)
                    .unwrap();
                request_sent = true;
            }
            loop {
                match http3_conn.poll(&mut conn) {
                    Ok((stream_id, h3::Event::Headers { list, .. })) => {
                        info!("got response headers {:?} on stream id {}", list, stream_id);
                    }
                    Ok((stream_id, h3::Event::Data)) => {
                        if let Ok(read) = http3_conn.recv_body(&mut conn, stream_id, &mut buf) {
                            debug!(
                                "got {} bytes of response data on stream {}",
                                read, stream_id
                            );
                            received_total += read;
                        }
                    }
                    Ok((stream_id, h3::Event::Finished)) => {
                        info!(
                            "response received in {:?} on stream id {}, body length {}, closing...",
                            request_begin_at.elapsed(),
                            stream_id,
                            received_total,
                        );
                        conn.close(true, 0x00, b"kthxbye").unwrap();
                    }
                    Ok((_flow_id, h3::Event::Datagram)) => (),
                    Ok((goaway_id, h3::Event::GoAway)) => {
                        info!("GOAWAY id={}", goaway_id);
                    }
                    Err(h3::Error::Done) => {
                        break;
                    }
                    Err(e) => {
                        error!("HTTP/3 processing failed: {:?}", e);
                        break;
                    }
                }
            }
        }

        loop {
            let written = match conn.send(&mut out) {
                Ok(v) => v,
                Err(quiche::Error::Done) => {
                    debug!("done writing");
                    break;
                }
                Err(err) => {
                    error!("send failed: {:?}", err);
                    conn.close(false, 0x1, b"fail").ok();
                    break;
                }
            };

            if let Err(e) = socket.send(&out[..written]) {
                if e.kind() == IOErrorKind::WouldBlock {
                    debug!("send() would block");
                    break;
                }

                bail!("send() failed: {:?}", e);
            }

            debug!("written {}", written);
        }

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
            break;
        }
    }

    Ok(())
}

fn make_quiche_config() -> anyhow::Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

    config.verify_peer(false);
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

    Ok(config)
}
