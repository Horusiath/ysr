use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use std::borrow::Cow;
use ysr::{BlockHeader, ID};

fn main() {
    println!(
        "SmallVec<[u8;16]>: {} bytes",
        size_of::<SmallVec<[u8; 16]>>()
    );
    println!("Vec<u8>: {} bytes", size_of::<Vec<u8>>());
    println!("Bytes: {} bytes", size_of::<Bytes>());
    println!("BytesMut: {} bytes", size_of::<BytesMut>());
    println!("Box<[u8]>: {} bytes", size_of::<Box<[u8]>>());
    println!("Cow<[u8]>: {} bytes", size_of::<Cow<'static, [u8]>>());
    println!("ID: {} bytes", size_of::<ID>());
    println!("Block Header: {} bytes", size_of::<BlockHeader>());
}
