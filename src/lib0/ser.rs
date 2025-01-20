use crate::lib0::{
    ExpectedString, TAG_ARRAY, TAG_BIGINT, TAG_BYTE_ARRAY, TAG_FALSE, TAG_FLOAT32, TAG_FLOAT64,
    TAG_INTEGER, TAG_NULL, TAG_OBJECT, TAG_STRING, TAG_TRUE, TAG_UNDEFINED,
};
use crate::write::WriteExt;
use serde::de::{Error, Expected, Unexpected};
use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::Serialize;
use std::io::Write;

pub(super) struct Serializer<W> {
    writer: W,
}

impl<W: Write> Serializer<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    fn serialize_variant(
        &mut self,
        variant: &'static str,
        param_len: usize,
    ) -> Result<(), super::Error> {
        self.writer.write_string(variant)?;
        self.writer.write_var(param_len)?;
        Ok(())
    }
}

impl<'a, W: Write> serde::ser::Serializer for &'a mut Serializer<W> {
    type Ok = ();
    type Error = super::Error;
    type SerializeSeq = SeqSerializer<'a, W>;
    type SerializeTuple = SeqSerializer<'a, W>;
    type SerializeTupleStruct = SeqSerializer<'a, W>;
    type SerializeTupleVariant = SeqSerializer<'a, W>;
    type SerializeMap = MapSerializer<'a, W>;
    type SerializeStruct = MapSerializer<'a, W>;
    type SerializeStructVariant = MapSerializer<'a, W>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        // TYPE 120/121: boolean (true/false)
        self.writer.write_u8(if v { TAG_TRUE } else { TAG_FALSE })?;
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        // TYPE 125: INTEGER
        self.writer.write_u8(TAG_INTEGER)?;
        self.writer.write_var(v)?;
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        // TYPE 125: INTEGER
        self.writer.write_u8(TAG_INTEGER)?;
        self.writer.write_var(v)?;
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        // TYPE 125: INTEGER
        self.writer.write_u8(TAG_INTEGER)?;
        self.writer.write_var(v)?;
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        if v <= super::F64_MAX_SAFE_INTEGER && v >= super::F64_MIN_SAFE_INTEGER {
            // TYPE 125: INTEGER
            self.writer.write_u8(TAG_INTEGER)?;
            self.writer.write_var(v)?;
        } else if ((v as f32) as i64) == v {
            // TYPE 124: FLOAT32
            self.writer.write_u8(TAG_FLOAT32)?;
            self.writer.write_f32(v as f32)?;
        } else if ((v as f64) as i64) == v {
            // TYPE 123: FLOAT64
            self.writer.write_u8(TAG_FLOAT64)?;
            self.writer.write_f64(v as f64)?;
        } else {
            // TYPE 122: BigInt
            self.writer.write_u8(TAG_BIGINT)?;
            self.writer.write_i64(v)?;
        }
        Ok(())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        // TYPE 125: INTEGER
        self.writer.write_u8(TAG_INTEGER)?;
        self.writer.write_var(v)?;
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        // TYPE 125: INTEGER
        self.writer.write_u8(TAG_INTEGER)?;
        self.writer.write_var(v)?;
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        // TYPE 125: INTEGER
        self.writer.write_u8(125)?;
        self.writer.write_var(v)?;
        Ok(())
    }

    fn serialize_u64(self, num: u64) -> Result<Self::Ok, Self::Error> {
        let v = num as i64;
        if (v as u64) != num {
            // loss of precision
            return Err(Error::invalid_value(
                Unexpected::Unsigned(num),
                &ExpectedString("integer within i64 bounds"),
            ));
        }
        self.serialize_i64(v)
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        // TYPE 124: FLOAT32
        self.writer.write_u8(TAG_FLOAT32)?;
        self.writer.write_f32(v)?;
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        // TYPE 123: FLOAT64
        self.writer.write_u8(TAG_FLOAT64)?;
        self.writer.write_f64(v)?;
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let str = v.to_string();
        self.serialize_str(&str)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        // TYPE 119: String
        self.writer.write_u8(TAG_STRING)?;
        self.writer.write_string(v)?;
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        // TYPE 116: Buffer
        self.writer.write_u8(TAG_BYTE_ARRAY)?;
        self.writer.write_bytes(v)?;
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        // TYPE 126: null
        Ok(self.writer.write_u8(TAG_NULL)?)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        // TYPE 127: undefined
        self.writer.write_u8(TAG_UNDEFINED)?;
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        // TYPE 127: undefined
        self.writer.write_u8(TAG_UNDEFINED)?;
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        /* same as serializing `{ "variant": undefined }` */

        // TYPE 118: Map
        self.writer.write_u8(TAG_OBJECT)?;
        self.writer.write_var(1)?;
        self.writer.write_string(variant)?;
        // TYPE 127: undefined
        ().serialize(self)?;
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        /* same as serializing `{ "variant": value }` */

        // TYPE 118: Map
        self.writer.write_u8(TAG_OBJECT)?;
        self.writer.write_var(1)?;
        self.writer.write_string(variant)?;
        // TYPE 127: undefined
        value.serialize(self)?;
        Ok(())
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match len {
            None => Err(super::Error::UnknownLength),
            Some(len) => SeqSerializer::new(self, len),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        SeqSerializer::new(self, len)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        SeqSerializer::new(self, len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        /* same as serializing `{ "variant": [a, b, c] }` */
        // TYPE 118: Map
        self.writer.write_u8(TAG_OBJECT)?;
        self.writer.write_var(1)?;
        self.writer.write_string(variant)?;
        SeqSerializer::new(self, len)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        match len {
            None => Err(super::Error::UnknownLength),
            Some(len) => MapSerializer::new(self, len),
        }
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        MapSerializer::new(self, len)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        /* same as serializing `{ "variant": { "a": b, "c": d } }` */
        // TYPE 118: Map
        self.writer.write_u8(TAG_OBJECT)?;
        self.writer.write_var(1)?;
        self.writer.write_string(variant)?;
        MapSerializer::new(self, len)
    }
}

pub(super) struct SeqSerializer<'a, W> {
    ser: &'a mut Serializer<W>,
}

impl<'a, W: Write> SeqSerializer<'a, W> {
    fn new(ser: &'a mut Serializer<W>, len: usize) -> Result<Self, super::Error> {
        // TYPE 117: Array
        ser.writer.write_u8(TAG_ARRAY)?;
        ser.writer.write_var(len)?;
        Ok(SeqSerializer { ser })
    }
}

impl<'a, W: Write> SerializeSeq for SeqSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write> SerializeTuple for SeqSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    #[inline]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write> SerializeTupleStruct for SeqSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    #[inline]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write> SerializeTupleVariant for SeqSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    #[inline]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

pub(super) struct MapSerializer<'a, W> {
    ser: &'a mut Serializer<W>,
}

impl<'a, W: Write> MapSerializer<'a, W> {
    fn new(ser: &'a mut Serializer<W>, len: usize) -> Result<Self, super::Error> {
        // TYPE 118: Map
        ser.writer.write_u8(TAG_OBJECT)?;
        ser.writer.write_var(len)?;
        Ok(MapSerializer { ser })
    }
}

impl<'a, W: Write> SerializeMap for MapSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(self)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write> SerializeStruct for MapSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut *self)?;
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write> SerializeStructVariant for MapSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut *self)?;
        value.serialize(&mut *self.ser)
    }

    #[inline]
    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, 'b, W: Write> serde::Serializer for &'b mut MapSerializer<'a, W> {
    type Ok = ();
    type Error = super::Error;
    type SerializeSeq = Impossible<(), super::Error>;
    type SerializeTuple = Impossible<(), super::Error>;
    type SerializeTupleStruct = Impossible<(), super::Error>;
    type SerializeTupleVariant = Impossible<(), super::Error>;
    type SerializeMap = Impossible<(), super::Error>;
    type SerializeStruct = Impossible<(), super::Error>;
    type SerializeStructVariant = Impossible<(), super::Error>;

    #[inline]
    fn serialize_bool(self, _: bool) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_i8(self, _: i8) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_i16(self, _: i16) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_i32(self, _: i32) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_i64(self, _: i64) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_u8(self, _: u8) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_u16(self, _: u16) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_u32(self, _: u32) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_u64(self, _: u64) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_f32(self, _: f32) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_f64(self, _: f64) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_char(self, _: char) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.ser.serialize_str(v)
    }

    #[inline]
    fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    #[inline]
    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(super::Error::NonStringKey)
    }
}
