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
const TASK_COUNT: usize = 10;
const WORKER_COUNT: usize = 10;
const REQ_CNT_FOR_EACH_TASK: usize = 10;
const SERVER_NAME: &str = "up.qiniu.com";
const UPLOAD_TOKEN: &str = "HwFOxpYCQU6oXoZXFOTh1mq5ZZig6Yyocgk3BTZZ:eJ4mCfwJjaQ_iycI2x6vk0qFiXA=:eyJkZWFkbGluZSI6MTY0OTIyNjI4NCwic2NvcGUiOiIyMDIwLTA2LWNoZWNrYmlsbHMifQ==";

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
                for i in 0..TASK_COUNT {
                    let tasks_number = tasks_number.to_owned();
                    let sender = sender.to_owned();
                    s.spawn(move |_| {
                        let _tasks_number_guard = TasksNumberGuard::new(tasks_number, sender);
                        for j in 0..REQ_CNT_FOR_EACH_TASK {
                            start_client_worker(bind_addr, server_address, &format!("{}-{}", i, j))
                                .expect("Client worker failed")
                        }
                    });
                }
            });

        Ok(())
    }
}
fn start_client_worker(
    bind_addr: SocketAddr,
    server_address: SocketAddr,
    task_id: &str,
) -> anyhow::Result<()> {
    let mut socket = mio::net::UdpSocket::bind(bind_addr)?;
    socket.connect(server_address)?;
    let random = SystemRandom::new();

    let mut poll = mio::Poll::new().unwrap();
    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

    let mut config = make_quiche_config().unwrap();
    let mut http3_conn = None;
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    random.fill(&mut scid[..]).unwrap();
    let scid = quiche::ConnectionId::from_ref(&scid);
    let mut conn = quiche::connect(Some(SERVER_NAME), &scid, &mut config).unwrap();
    info!(
        "[{}] connecting to {:} from {:} with scid {}",
        task_id,
        server_address,
        socket.local_addr().unwrap(),
        hex::encode(&scid)
    );

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];
    let written = conn.send(&mut out).context("initial send failed")?;

    while let Err(err) = socket.send(&out[..written]) {
        if err.kind() == IOErrorKind::WouldBlock {
            debug!("[{}] send() would block", task_id);
            continue;
        }

        bail!("[{}] send() failed: {:?}", task_id, err);
    }
    debug!("[{}] written {}", task_id, written);

    let h3_config = quiche::h3::Config::new().unwrap();
    const FILE_SIZE: usize = 1 << 20;
    let request_headers = vec![
        h3::Header::new(":method", "POST"),
        h3::Header::new(":scheme", "https"),
        h3::Header::new(":authority", SERVER_NAME),
        h3::Header::new(":path", &format!("/put/{}", FILE_SIZE)),
        h3::Header::new("user-agent", "quiche-test-client"),
        h3::Header::new("authorization", &format!("UpToken {}", UPLOAD_TOKEN)),
    ];
    let request_begin_at = Instant::now();
    let mut stream_id = None;
    let mut request_body_sent = 0usize;
    let mut received_total = 0usize;
    let mut request_body = vec![0u8; FILE_SIZE];
    random.fill(&mut request_body).unwrap();

    let mut events = mio::Events::with_capacity(1024);

    loop {
        poll.poll(&mut events, conn.timeout())?;

        'read: loop {
            if events.is_empty() {
                debug!("[{}] timed out", task_id);
                conn.on_timeout();
                break 'read;
            }

            let len = match socket.recv(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == IOErrorKind::WouldBlock {
                        debug!("[{}] recv() would block", task_id);
                        break 'read;
                    }
                    bail!("[{}] recv() failed: {:?}", task_id, e);
                }
            };
            debug!("[{}] got {} bytes", task_id, len);

            let read = match conn.recv(&mut buf[..len]) {
                Ok(v) => v,
                Err(e) => {
                    error!("[{}] recv failed: {:?}", task_id, e);
                    continue 'read;
                }
            };

            debug!("[{}] processed {} bytes", task_id, read);
        }

        debug!("[{}] done reading", task_id);

        if conn.is_closed() {
            info!("[{}] connection closed, {:?}", task_id, conn.stats());
            break;
        }

        if conn.is_established() && http3_conn.is_none() {
            http3_conn = Some(h3::Connection::with_transport(&mut conn, &h3_config)?);
        }

        if let Some(http3_conn) = &mut http3_conn {
            if stream_id.is_none() {
                info!("[{}] sending HTTP request {:?}", task_id, request_headers);

                stream_id = Some(
                    http3_conn
                        .send_request(&mut conn, &request_headers, false)
                        .unwrap(),
                );
            }
            if let Some(stream_id) = stream_id {
                if request_body_sent < request_body.len() {
                    let body = &request_body[request_body_sent..];
                    match http3_conn.send_body(&mut conn, stream_id, body, true) {
                        Ok(written) => {
                            debug!("[{}] Send {} bytes for body", task_id, written);
                            request_body_sent += written;
                        }
                        Err(h3::Error::Done) => {
                            debug!("[{}] Wait for a while to send body", task_id);
                        }
                        Err(err) => {
                            bail!("[{}] Failed to send body: {}", task_id, err);
                        }
                    };
                }
            }
            loop {
                match http3_conn.poll(&mut conn) {
                    Ok((stream_id, h3::Event::Headers { list, .. })) => {
                        info!(
                            "[{}] got response headers {:?} on stream id {}",
                            task_id, list, stream_id
                        );
                    }
                    Ok((stream_id, h3::Event::Data)) => {
                        if let Ok(read) = http3_conn.recv_body(&mut conn, stream_id, &mut buf) {
                            debug!(
                                "[{}] got {} bytes of response data on stream {}",
                                task_id, read, stream_id
                            );
                            received_total += read;
                        }
                    }
                    Ok((stream_id, h3::Event::Finished)) => {
                        info!(
                            "[{}] response received in {:?} on stream id {}, body length {}, closing...", task_id,
                            request_begin_at.elapsed(),
                            stream_id,
                            received_total,
                        );
                        conn.close(true, 0x00, b"kthxbye").unwrap();
                    }
                    Ok((_flow_id, h3::Event::Datagram)) => (),
                    Ok((goaway_id, h3::Event::GoAway)) => {
                        info!("[{}] GOAWAY id={}", task_id, goaway_id);
                    }
                    Err(h3::Error::Done) => {
                        break;
                    }
                    Err(e) => {
                        error!("[{}] HTTP/3 processing failed: {:?}", task_id, e);
                        break;
                    }
                }
            }
        }

        loop {
            let written = match conn.send(&mut out) {
                Ok(v) => v,
                Err(quiche::Error::Done) => {
                    debug!("[{}] done writing", task_id);
                    break;
                }
                Err(err) => {
                    error!("[{}] send failed: {:?}", task_id, err);
                    conn.close(false, 0x1, b"fail").ok();
                    break;
                }
            };

            if let Err(e) = socket.send(&out[..written]) {
                if e.kind() == IOErrorKind::WouldBlock {
                    debug!("[{}] send() would block", task_id);
                    break;
                }

                bail!("[{}] send() failed: {:?}", task_id, e);
            }

            debug!("[{}] written {}", task_id, written);
        }

        if conn.is_closed() {
            info!("[{}] connection closed, {:?}", task_id, conn.stats());
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
