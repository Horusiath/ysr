use crate::lib0::Value;
use crate::{ListPrelim, MapPrelim};

#[derive(Debug, Clone, PartialEq)]
pub enum In {
    Value(Value),
    List(ListPrelim),
    Map(MapPrelim),
}

impl In {
    pub fn is_empty(&self) -> bool {
        match self {
            In::Value(Value::Array(vals)) => vals.is_empty(),
            In::Value(Value::String(str)) => str.is_empty(),
            In::Value(Value::Object(map)) => map.is_empty(),
            In::List(prelim) => prelim.is_empty(),
            In::Map(prelim) => prelim.is_empty(),
            _ => false,
        }
    }
}

impl From<ListPrelim> for In {
    fn from(value: ListPrelim) -> Self {
        In::List(value)
    }
}

impl From<MapPrelim> for In {
    fn from(value: MapPrelim) -> Self {
        In::Map(value)
    }
}

impl<T> From<T> for In
where
    T: Into<Value>,
{
    fn from(value: T) -> Self {
        In::Value(value.into())
    }
}
