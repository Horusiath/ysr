use crate::U64;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

fn roundtrip<T>(value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let mut buf = Vec::new();
    super::to_writer(&mut buf, value).unwrap();
    let deserialized = super::from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
    assert_eq!(value, &deserialized);
}

#[test]
fn serialize_deserialize() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestData {
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
        fileds: Vec<NamedFieldEnum>,
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
    enum NamedFieldEnum {
        A { x: u32 },
        B { y: f64 },
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestNestedData {
        name: Arc<str>,
        age: Option<u64>,
    }

    let data = TestData {
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
        fileds: vec![NamedFieldEnum::A { x: 100 }, NamedFieldEnum::B { y: 200.0 }],
    };

    roundtrip(&data);
}
