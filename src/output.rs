use crate::lib0::Value;
use crate::node::NodeID;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Out {
    Value(Value),
    Node(NodeID),
}
