use crate::lib0::Value;
use crate::{ListPrelim, MapPrelim};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum In {
    Value(Value),
    List(ListPrelim),
    Map(MapPrelim),
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
