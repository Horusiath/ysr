use crate::content::Content;
use crate::lib0::Value;
use crate::node::NodeType;
use crate::transaction::TxMutScope;
use crate::{BlockMut, Clock, In, Out};
use serde::Serialize;
use smallvec::{SmallVec, smallvec};

pub trait Prelim {
    type Return;

    fn clock_len(&self) -> Clock;

    fn prepare(&self) -> crate::Result<Prepare>;

    fn integrate<'tx>(
        self,
        parent: &mut BlockMut,
        tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return>;
}

pub enum Prepare {
    Node(NodeType),
    Values(SmallVec<[Content<'static>; 1]>),
}

impl<T> Prelim for T
where
    T: Serialize + 'static,
{
    type Return = ();

    #[inline]
    fn clock_len(&self) -> Clock {
        Clock::new(1)
    }

    fn prepare(&self) -> crate::Result<Prepare> {
        Ok(Prepare::Values(smallvec![Content::atom(self)?]))
    }

    fn integrate<'tx>(
        self,
        _parent: &mut BlockMut,
        _tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return> {
        Ok(())
    }
}

#[repr(transparent)]
pub(crate) struct DeltaPrelim(pub In);
impl Prelim for DeltaPrelim {
    type Return = ();

    fn clock_len(&self) -> Clock {
        match &self.0 {
            In::Value(Value::String(str)) => {
                let utf16_len = str.encode_utf16().count();
                Clock::new(utf16_len as u32)
            }
            _ => Clock::new(1),
        }
    }

    fn prepare(&self) -> crate::Result<Prepare> {
        match &self.0 {
            In::Value(Value::String(str)) => Ok(Prepare::Values(smallvec![Content::string(str)])),
            In::Value(value) => Ok(Prepare::Values(smallvec![Content::embed(value)?])),
            In::List(prelim) => prelim.prepare(),
            In::Map(prelim) => prelim.prepare(),
        }
    }

    fn integrate<'tx>(
        self,
        parent: &mut BlockMut,
        tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return> {
        match self.0 {
            In::Value(_) => { /* ignore */ }
            In::List(prelim) => {
                prelim.integrate(parent, tx)?;
            }
            In::Map(prelim) => {
                prelim.integrate(parent, tx)?;
            }
        }
        Ok(())
    }
}

impl Prelim for In {
    type Return = Out;

    #[inline]
    fn clock_len(&self) -> Clock {
        Clock::new(1)
    }

    fn prepare(&self) -> crate::Result<Prepare> {
        match self {
            In::Value(value) => value.prepare(),
            In::List(prelim) => prelim.prepare(),
            In::Map(prelim) => prelim.prepare(),
        }
    }

    fn integrate<'tx>(
        self,
        parent: &mut BlockMut,
        tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return> {
        match self {
            In::Value(value) => Ok(Out::Value(value)),
            In::List(prelim) => Ok(Out::Node(prelim.integrate(parent, tx)?.node_id())),
            In::Map(prelim) => Ok(Out::Node(prelim.integrate(parent, tx)?.node_id())),
        }
    }
}

#[repr(transparent)]
pub(crate) struct StringPrelim<'a> {
    data: &'a str,
}
impl<'a> StringPrelim<'a> {
    pub fn new(data: &'a str) -> Self {
        StringPrelim { data }
    }
}
impl<'a> Prelim for StringPrelim<'a> {
    type Return = ();

    fn clock_len(&self) -> Clock {
        let utf16_len = self.data.encode_utf16().count();
        Clock::new(utf16_len as u32)
    }

    fn prepare(&self) -> crate::Result<Prepare> {
        Ok(Prepare::Values(smallvec![Content::string(self.data)]))
    }

    fn integrate<'tx>(
        self,
        _parent: &mut BlockMut,
        _tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return> {
        Ok(())
    }
}
