use smallvec::SmallVec;
use ysr::{BlockHeader, ID};

fn main() {
    println!(
        "SmallVec<[u8;16]>: {} bytes",
        size_of::<SmallVec<[u8; 16]>>()
    );
    println!("ID: {} bytes", size_of::<ID>());
    println!("Block Header: {} bytes", size_of::<BlockHeader>());
}
