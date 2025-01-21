use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

fn roundtrip<T>(value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    // 1. serialize object
    let mut buf = Vec::new();
    super::to_writer(&mut buf, value).unwrap();

    // 2. copy object over
    let mut buf2 = Vec::new();
    super::copy(&mut Cursor::new(&buf), &mut buf2).unwrap();
    assert_eq!(buf, buf2);

    // 3. read object from copy
    let deserialized: T = super::from_reader(&mut Cursor::new(buf2)).unwrap();
    assert_eq!(value, &deserialized);
}

#[test]
fn serialize_numbers() {
    roundtrip(&-127i8);
    roundtrip(&15_000i16);
    roundtrip(&2_000_000_000i32);
    roundtrip(&-9_000_000_000i64);
    roundtrip(&-9_000_000_000isize);
    roundtrip(&255u8);
    roundtrip(&65_000u16);
    roundtrip(&4_000_000_000u32);
    roundtrip(&9_000_000_000u64);
    roundtrip(&9_000_000_000usize);
    roundtrip(&0.5f32);
    roundtrip(&0.333f64);
}

#[test]
fn serialize_string() {
    roundtrip(&"hello".to_string());
}

#[test]
fn serialize_adt() {
    roundtrip(&TestEnum::A);
    roundtrip(&ADT::C(Some(56.7)));
    roundtrip(&MultiFieldADT::A(100, 200));
    roundtrip(&NamedFieldEnum::B { y: 10.0 })
}

#[test]
fn serialize_deserialize() {
    let data = TestData {
        truthy: true,
        falsey: false,
        i8: -127,
        i16: 15_000,
        i32: 2_000_000_000,
        i64: -9_000_000_000,
        isize: -9_000_000_000,
        u8: 255,
        u16: 65_000,
        u32: 4_000_000_000,
        u64: 9_000_000_000,
        usize: 9_000_000_000,
        f32: 0.5,
        f64: 0.333,
        str: "hello".to_string(),
        buf: b"deadbeef".into(),
        unit: (),
        tuple: (123, "world".to_string()),
        nested: TestNestedData {
            name: "John Doe".into(),
            age: None,
        },
        array: vec![
            TestNestedData {
                name: "Smith".into(),
                age: Some(18),
            },
            TestNestedData {
                name: "Smith".into(),
                age: None,
            },
        ],
        enum_struct1: TestEnum::A,
        alias: Millis(100),
        point: Point(15.52, 54.32),
        adts: vec![
            ADT::A(8000),
            ADT::B("hello".to_string()),
            ADT::C(Some(56.7)),
        ],
        fields: HashMap::from([
            ("A".to_string(), NamedFieldEnum::A { x: 100 }),
            ("B".to_string(), NamedFieldEnum::B { y: 200.0 }),
        ]),
    };

    roundtrip(&data);
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TestData {
    truthy: bool,
    falsey: bool,
    i8: i8,
    i16: i16,
    i32: i32,
    i64: i64,
    isize: isize,
    u8: u8,
    u16: u16,
    u32: u32,
    u64: u64,
    usize: usize,
    f32: f32,
    f64: f64,
    str: String,
    buf: Vec<u8>,
    unit: (),
    tuple: (i32, String),
    nested: TestNestedData,
    array: Vec<TestNestedData>,
    enum_struct1: TestEnum,
    alias: Millis,
    point: Point,
    adts: Vec<ADT>,
    fields: HashMap<String, NamedFieldEnum>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Millis(u64);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Point(f32, f32);

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum TestEnum {
    A,
    B,
    C,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum ADT {
    A(u32),
    B(String),
    C(Option<f32>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum MultiFieldADT {
    A(u32, u32),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum NamedFieldEnum {
    A { x: u32 },
    B { y: f64 },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TestNestedData {
    name: Arc<str>,
    age: Option<u64>,
}
