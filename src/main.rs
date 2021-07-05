mod client;
mod server;
mod tasks_number;

use client::{new_tasks_number, start_clients};
use server::start_server;

use std::{
    io::Result as IOResult,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread::Builder as ThreadBuilder,
};
const MAX_DATAGRAM_SIZE: usize = 1350;
const ONE_GB: u64 = 1 << 30;

fn main() -> IOResult<()> {
    env_logger::init();

    let server_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 4433);
    let server_socket = mio::net::UdpSocket::bind(server_address)?;
    let tasks_number = new_tasks_number();
    let (sender, receiver) = mio::unix::pipe::new()?;

    let mut threads = Vec::with_capacity(2);
    threads.push({
        let tasks_number = tasks_number.to_owned();
        ThreadBuilder::new()
            .name("quiche server".to_owned())
            .spawn(move || {
                start_server(server_socket, tasks_number, receiver);
            })
            .unwrap()
    });
    threads.push({
        let tasks_number = tasks_number.to_owned();
        ThreadBuilder::new()
            .name("quiche clients".to_owned())
            .spawn(move || {
                start_clients(server_address, tasks_number, sender);
            })
            .unwrap()
    });

    for thread in threads {
        thread.join().unwrap();
    }

    Ok(())
}
