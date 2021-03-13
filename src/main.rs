mod client;
mod server;

use client::start_clients;
use server::start_server;

use std::{
    io::Result as IOResult,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    thread::Builder as ThreadBuilder,
};

fn main() -> IOResult<()> {
    env_logger::init();

    let server_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4433);
    let socket = mio::net::UdpSocket::bind(server_address)?;

    let mut threads = Vec::with_capacity(2);
    threads.push(
        ThreadBuilder::new()
            .name("quiche server".to_owned())
            .spawn(move || {
                start_server(socket);
            })
            .unwrap(),
    );
    // threads.push(
    //     ThreadBuilder::new()
    //         .name("quiche clients".to_owned())
    //         .spawn(move || {
    //             start_clients();
    //         })
    //         .unwrap(),
    // );

    for thread in threads {
        thread.join().unwrap();
    }

    Ok(())
}
