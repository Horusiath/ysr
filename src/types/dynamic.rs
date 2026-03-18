use crate::lib0::Value;
use crate::node::NodeType;
use crate::types::Capability;
use crate::{ListRef, MapRef, Mounted, TextRef, Transaction, lib0};

pub type DynRef<Txn> = Mounted<Dyn, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct Dyn;

impl Capability for Dyn {
    fn node_type() -> NodeType {
        NodeType::Unknown
    }
}

impl<'tx, 'db> DynRef<&'tx Transaction<'db>> {
    pub fn to_value(&self) -> crate::Result<Value> {
        let node_type = self
            .block
            .node_type()
            .ok_or_else(|| crate::Error::Custom("mounted block doesn't belong to node".into()))?;

        match node_type {
            NodeType::Unknown => Ok(Value::Undefined),
            NodeType::List => {
                let list: ListRef<_> = Mounted::new(self.block.clone(), self.tx);
                list.to_value()
            }
            NodeType::Map => {
                let map: MapRef<_> = Mounted::new(self.block.clone(), self.tx);
                map.to_value()
            }
            NodeType::Text => {
                let text: TextRef<_> = Mounted::new(self.block.clone(), self.tx);
                Ok(Value::String(text.to_string()))
            }
            NodeType::XmlFragment | NodeType::XmlElement | NodeType::XmlText => {
                unimplemented!();
            }
        }
    }
}
