use crate::lib0::Value;
use crate::node::NodeID;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Out {
    Value(Value),
    Node(NodeID),
}

impl Out {
    #[inline]
    pub fn is_value(&self) -> bool {
        matches!(self, Out::Value(_))
    }

    #[inline]
    pub fn is_node(&self) -> bool {
        matches!(self, Out::Node(_))
    }

    #[inline]
    pub fn as_value(&self) -> Option<&Value> {
        if let Out::Value(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn as_node(&self) -> Option<&NodeID> {
        if let Out::Node(n) = self {
            Some(n)
        } else {
            None
        }
    }
}

impl<T> From<T> for Out
where
    T: Into<Value>,
{
    #[inline]
    fn from(value: T) -> Self {
        Out::Value(value.into())
    }
}
