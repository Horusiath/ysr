use crate::block::InsertBlockData;
use crate::lib0::Value;
use crate::prelim::Prelim;
use crate::{ListPrelim, MapPrelim, Out, Transaction};

#[derive(Debug, Clone, PartialEq)]
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

impl Prelim for In {
    type Return = Out;

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()> {
        match self {
            In::Value(value) => value.prepare(insert),
            In::List(list) => list.prepare(insert),
            In::Map(map) => map.prepare(insert),
        }
    }

    fn integrate(
        self,
        insert: &mut InsertBlockData,
        tx: &mut Transaction,
    ) -> crate::Result<Self::Return> {
        match self {
            In::Value(value) => Ok(Out::Value(value)),
            In::List(list) => {
                list.integrate(insert, tx)?;
                Ok(Out::Node(*insert.block.id()))
            }
            In::Map(map) => {
                map.integrate(insert, tx)?;
                Ok(Out::Node(*insert.block.id()))
            }
        }
    }
}
