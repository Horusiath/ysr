use crate::block::{ID, InsertBlockData};
use crate::content::{Content, ContentType};
use crate::lib0::Value;
use crate::node::NodeType;
use crate::prelim::{DeltaPrelim, Prelim, StringPrelim};
use crate::state_vector::Snapshot;
use crate::store::Db;
use crate::store::block_store::{BlockCursor, SplitResult};
use crate::store::content_store::ContentStore;
use crate::transaction::{TransactionState, TxMutScope, TxScope};
use crate::types::Capability;
use crate::{Block, BlockMut, Clock, In, Mounted, Out, Prepare, Transaction, lib0};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use std::borrow::Cow;
use std::collections::{BTreeMap, Bound};
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::ops::{Deref, RangeBounds};

pub type TextRef<Txn> = Mounted<Text, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct Text;

impl Capability for Text {
    fn node_type() -> NodeType {
        NodeType::Text
    }
}

impl<'db, 'tx: 'db> TextRef<&'tx Transaction<'db>> {
    pub fn len(&self) -> usize {
        self.block.node_len()
    }

    /// Returns an iterator over uncommitted changes (deltas) made to this text type
    /// within its current transaction scope.
    pub fn uncommitted(&self) -> Uncommitted<'tx> {
        let tx = self.tx.read_context().unwrap();
        let state = self.tx.state.get();
        Uncommitted::new(self.block.start().copied(), tx, state)
    }

    /// Returns an iterator over all text and embedded chunks grouped by their applied attributes.
    pub fn chunks(&self) -> Chunks<'db, 'tx> {
        self.chunks_between(None, None)
    }

    /// Returns an iterator over all text and embedded chunks grouped by their applied attributes,
    /// scoped between two provided snapshots.
    pub fn chunks_between<'a>(
        &self,
        from: Option<&'a Snapshot>,
        to: Option<&'a Snapshot>,
    ) -> Chunks<'a, 'tx> {
        let tx = self
            .tx
            .read_context()
            .expect("todo: handle errors in chunk iterator creation");
        let start = self.block.start().copied();

        Chunks::new(tx, start, from, to)
    }
}

/// Individual chunk of data produced when calling [TextRef::chunks]/[TextRef::chunks_between] iterator.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Chunk {
    pub insert: Out,
    pub attributes: Option<Box<Attrs>>,
    pub operation: Option<Op>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Insert(ID),
    Delete(ID),
}

impl Chunk {
    pub fn new<O: Into<Out>>(insert: O) -> Self {
        Self {
            insert: insert.into(),
            attributes: None,
            operation: None,
        }
    }

    pub fn with_attrs(self, attrs: Attrs) -> Self {
        Self {
            operation: self.operation,
            insert: self.insert,
            attributes: Some(Box::new(attrs)),
        }
    }

    pub fn with_op(mut self, op: Op) -> Self {
        self.operation = Some(op);
        self
    }
}

impl<'tx, 'db> Display for TextRef<&'tx Transaction<'db>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut next = self.block.start().copied();
        let db = self.tx.db.get();
        let blocks = db.blocks();
        let mut cursor = blocks.cursor().map_err(|_| std::fmt::Error)?;
        let contents = db.contents();
        while let Some(right_id) = next {
            // right id should always point at the beginning of the block, so
            // direct seek should be fine
            let block = cursor.seek(right_id).map_err(|_| std::fmt::Error)?;
            if block.is_countable()
                && !block.is_deleted()
                && block.content_type() == ContentType::String
            {
                let data = get_content(&block, &contents)?;
                if let Ok(str) = data.as_str() {
                    str.fmt(f)?;
                }
            }
            next = block.right().cloned();
        }

        Ok(())
    }
}
impl<'db, 'tx> TextRef<&'tx mut Transaction<'db>> {
    fn insert_at<P>(
        tx: &mut TxMutScope<'_>,
        pos: &mut BlockPosition,
        value: P,
        attrs: Option<Box<Attrs>>,
    ) -> crate::Result<P::Return>
    where
        P: Prelim,
    {
        let negated = if let Some(mut attrs) = attrs {
            pos.unset_missing(&mut attrs);
            pos.minimize(&attrs, &mut tx.cursor)?;
            pos.insert_attributes(tx, attrs)?
        } else {
            Attrs::new()
        };

        let result = pos.insert_internal(tx, value)?;

        if !negated.is_empty() {
            pos.insert_negated(tx, negated)?;
        }

        Ok(result)
    }

    fn format_at(
        tx: &mut TxMutScope<'_>,
        pos: &mut BlockPosition,
        len: usize,
        attrs: Option<Box<Attrs>>,
    ) -> crate::Result<()> {
        if let Some(attrs) = attrs
            && !attrs.is_empty()
        {
            let mut remaining = len as u32;
            pos.minimize(&attrs, &mut tx.cursor)?;
            let mut negated = pos.insert_attributes(tx, attrs.clone())?;

            while let Some(id) = pos.right {
                let right = tx.cursor.seek(id)?;
                if !(remaining != 0 || (!negated.is_empty() && Self::is_valid_target(&right))) {
                    break;
                }

                if !right.is_deleted() {
                    match right.content_type() {
                        ContentType::Format => {
                            let contents = tx.db.contents();
                            let content = get_content(&right, &contents)?;
                            let fmt = content.as_format()?;
                            let key = fmt.key();
                            if let Some(curr_value) = attrs.get(key) {
                                let value = fmt.value()?;
                                if curr_value == &value {
                                    negated.remove(key);
                                } else {
                                    negated.insert(key.into(), value);
                                }
                                tx.delete(&mut right.into(), false)?;
                            }
                        }
                        _ => {
                            let block_len = right.clock_len().get();
                            if remaining < block_len {
                                // split block
                                match tx.cursor.split_current(Clock::new(remaining))? {
                                    SplitResult::Unchanged(left) => {
                                        pos.left = Some(left.last_id());
                                        pos.right = None;
                                    }
                                    SplitResult::Split(left, right) => {
                                        pos.left = Some(left.last_id());
                                        pos.right = Some(*right.id());
                                    }
                                }
                                break;
                            } else {
                                remaining -= block_len;
                            }
                        }
                    }
                }

                forward(pos, &mut tx.cursor)?;
            }

            pos.insert_negated(tx, negated)?;
            Ok(())
        } else {
            pos.forward_by(len, &mut tx.cursor)
        }
    }

    fn is_valid_target(block: &Block<'_>) -> bool {
        if block.is_deleted() {
            true
        } else {
            block.content_type() == ContentType::Format
        }
    }

    fn remove_at(
        tx: &mut TxMutScope<'_>,
        pos: &mut BlockPosition,
        len: usize,
    ) -> crate::Result<()> {
        let mut remaining = len;
        let start = pos.right;
        let start_attrs = pos.attrs.clone();

        let mut deleted_count: u32 = 0;
        while let Some(block_id) = pos.right
            && remaining != 0
        {
            let block = tx.cursor.seek(block_id)?;
            if !block.is_deleted() {
                match block.content_type() {
                    ContentType::String | ContentType::Embed | ContentType::Node => {
                        let mut block: BlockMut = block.into();
                        let len = block.clock_len().get() as usize;
                        let to_delete = if remaining < len {
                            // split block (and the matching content store entry)
                            let split_result =
                                tx.cursor.split_current((remaining as u32).into())?;
                            block = match split_result {
                                SplitResult::Unchanged(block) => block,
                                SplitResult::Split(left, _) => left,
                            };
                            let n = remaining;
                            remaining = 0;
                            n
                        } else {
                            remaining -= len;
                            len
                        };
                        if tx.delete(&mut block, false)? {
                            deleted_count += to_delete as u32;
                        }
                    }
                    _ => { /* ignore */ }
                }
            }

            forward(pos, &mut tx.cursor)?;
        }

        if remaining != 0 {
            return Err(crate::Error::OutOfRange);
        }

        if let Some(start) = start.as_ref()
            && !start_attrs.is_empty()
            && !pos.attrs.is_empty()
        {
            clean_format_gap(tx, start, pos.right.as_ref(), &start_attrs, &mut pos.attrs)?;
        }

        if deleted_count > 0 {
            let parent_len = pos.parent.node_len() as u32 - deleted_count;
            pos.parent.set_node_len(parent_len);
            tx.cursor.update(pos.parent.as_block())?;
        }

        Ok(())
    }

    pub fn insert<S>(&mut self, utf16_index: usize, chunk: S) -> crate::Result<()>
    where
        S: AsRef<str>,
    {
        let chunk = chunk.as_ref();
        if chunk.is_empty() {
            return Ok(());
        }

        let mut tx = self.tx.write_context()?;
        let value = StringPrelim::new(chunk);
        let mut pos = BlockPosition::seek(&mut tx.cursor, &mut self.block, utf16_index)?;
        Self::insert_at(&mut tx, &mut pos, value, None)?;
        Ok(())
    }

    pub fn insert_with<S1, S2, A, V>(
        &mut self,
        utf16_index: usize,
        chunk: S1,
        attrs: A,
    ) -> crate::Result<()>
    where
        S1: AsRef<str>,
        S2: Into<String>,
        V: Into<Value>,
        A: IntoIterator<Item = (S2, V)>,
    {
        let chunk = chunk.as_ref();
        if chunk.is_empty() {
            return Ok(());
        }
        let attrs: Attrs = attrs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let mut tx = self.tx.write_context()?;
        let mut pos = BlockPosition::seek(&mut tx.cursor, &mut self.block, utf16_index)?;
        Self::insert_at(
            &mut tx,
            &mut pos,
            StringPrelim::new(chunk.as_ref()),
            Some(Box::new(attrs)),
        )
    }

    pub fn insert_embed<V>(&mut self, utf16_index: usize, value: V) -> crate::Result<V::Return>
    where
        V: Prelim,
    {
        let mut tx = self.tx.write_context()?;
        let mut pos = BlockPosition::seek(&mut tx.cursor, &mut self.block, utf16_index)?;
        Self::insert_at(&mut tx, &mut pos, EmbedPrelim(value), None)
    }

    pub fn insert_embed_with<S, A, P, V2>(
        &mut self,
        utf16_index: usize,
        value: P,
        attrs: A,
    ) -> crate::Result<P::Return>
    where
        S: Into<String>,
        P: Prelim,
        V2: Into<Value>,
        A: IntoIterator<Item = (S, V2)>,
    {
        let attrs: Attrs = attrs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        let mut tx = self.tx.write_context()?;
        let mut pos = BlockPosition::seek(&mut tx.cursor, &mut self.block, utf16_index)?;
        Self::insert_at(&mut tx, &mut pos, EmbedPrelim(value), Some(Box::new(attrs)))
    }

    pub fn push<S>(&mut self, chunk: S) -> crate::Result<()>
    where
        S: AsRef<str>,
    {
        let len = self.len();
        self.insert(len, chunk)
    }

    pub fn remove_range<R>(&mut self, utf16_range: R) -> crate::Result<()>
    where
        R: RangeBounds<usize>,
    {
        let start = match utf16_range.start_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index + 1,
            Bound::Unbounded => 0,
        };
        let end = match utf16_range.end_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index - 1,
            Bound::Unbounded => self.block.node_len(),
        };

        if start > end {
            return Ok(());
        }
        let remove_len = end - start + 1;
        let mut tx = self.tx.write_context()?;
        let mut pos = BlockPosition::seek(&mut tx.cursor, &mut self.block, start)?;
        Self::remove_at(&mut tx, &mut pos, remove_len)?;
        Ok(())
    }

    pub fn format<A, S, V, R>(&mut self, utf16_range: R, attrs: A) -> crate::Result<()>
    where
        A: IntoIterator<Item = (S, V)>,
        S: Into<String>,
        V: Into<Value>,
        R: RangeBounds<usize>,
    {
        let attrs: Attrs = attrs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        if attrs.is_empty() {
            return Ok(());
        }

        let start = match utf16_range.start_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index + 1,
            Bound::Unbounded => 0,
        };
        let end = match utf16_range.end_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index - 1,
            Bound::Unbounded => self.block.node_len(),
        };

        if end < start {
            return Ok(());
        }

        let len = end - start + 1;
        let mut tx = self.tx.write_context()?;

        let mut pos = BlockPosition::seek(&mut tx.cursor, &mut self.block, start)?;
        Self::format_at(&mut tx, &mut pos, len, Some(Box::new(attrs)))
    }

    pub fn apply_delta<I>(&mut self, delta: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = Delta<In>>,
    {
        let mut pos = BlockPosition::new(&mut self.block);
        let mut tx = self.tx.write_context()?;
        for delta in delta {
            match delta {
                Delta::Insert(value, fmt) => {
                    if !value.is_empty() {
                        Self::insert_at(&mut tx, &mut pos, DeltaPrelim(value), fmt)?;
                    }
                }
                Delta::Delete(len) => Self::remove_at(&mut tx, &mut pos, len)?,
                Delta::Retain(len, fmt) => Self::format_at(&mut tx, &mut pos, len, fmt)?,
            }
        }
        Ok(())
    }
}

impl<'tx, 'db> Deref for TextRef<&'tx mut Transaction<'db>> {
    type Target = TextRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const TextRef<&'tx Transaction<'db>>) }
    }
}

struct EmbedPrelim<T>(T);
impl<T: Prelim> Prelim for EmbedPrelim<T> {
    type Return = T::Return;

    fn clock_len(&self) -> Clock {
        self.0.clock_len()
    }

    fn prepare(&self) -> crate::Result<Prepare> {
        let prepared = self.0.prepare()?;
        match prepared {
            Prepare::Values(mut values)
                if values.len() == 1 && values[0].content_type() == ContentType::Atom =>
            {
                let value = values.pop().unwrap();
                Ok(Prepare::Values(smallvec![Content::new(
                    ContentType::Embed,
                    value.data
                )]))
            }
            other => Ok(other),
        }
    }

    fn integrate<'tx>(
        self,
        parent: &mut BlockMut,
        tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return> {
        self.0.integrate(parent, tx)
    }
}

pub struct FormatPrelim<'t, T> {
    key: &'t str,
    value: Option<T>,
}

impl<'t, T> FormatPrelim<'t, T> {
    pub fn new(key: &'t str, value: T) -> Self {
        FormatPrelim {
            key,
            value: Some(value),
        }
    }
}

impl<'t, T> Prelim for FormatPrelim<'t, T>
where
    T: Serialize,
{
    type Return = ();

    #[inline]
    fn clock_len(&self) -> Clock {
        Clock::new(1)
    }

    fn prepare(&self) -> crate::Result<Prepare> {
        Ok(Prepare::Values(smallvec![Content::format(
            self.key,
            &self.value
        )?]))
    }

    fn integrate<'tx>(
        self,
        parent: &mut BlockMut,
        tx: &mut TxMutScope<'tx>,
    ) -> crate::Result<Self::Return> {
        Ok(())
    }
}

pub type Attrs = BTreeMap<String, Value>;

/// A single change done over a text-like types: [Text] or [XmlText].
#[derive(Debug, Clone, PartialEq)]
pub enum Delta<T = Out> {
    /// Determines a change that resulted in insertion of a piece of text, which optionally could
    /// have been formatted with provided set of attributes.
    Insert(T, Option<Box<Attrs>>),

    /// Determines a change that resulted in removing a consecutive range of characters.
    Delete(usize),

    /// Determines a number of consecutive unchanged characters. Used to recognize non-edited spaces
    /// between [Delta::Insert] and/or [Delta::Delete] chunks. Can contain an optional set of
    /// attributes, which have been used to format an existing piece of text.
    Retain(usize, Option<Box<Attrs>>),
}

impl<T> Delta<T> {
    pub fn map<U, F>(self, f: F) -> Delta<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Delta::Insert(value, attrs) => Delta::Insert(f(value), attrs),
            Delta::Delete(len) => Delta::Delete(len),
            Delta::Retain(len, attrs) => Delta::Retain(len, attrs),
        }
    }

    pub fn with_attrs(self, attrs: Option<Box<Attrs>>) -> Self {
        match self {
            Delta::Insert(insert, _) => Delta::Insert(insert, attrs),
            Delta::Retain(len, _) => Delta::Retain(len, attrs),
            delete => delete,
        }
    }
}

pub struct Uncommitted<'tx> {
    tx: TxScope<'tx>,

    /// Transaction state. If `None`, means that the transaction was readonly and has no uncommitted
    /// changes.
    tx_state: Option<&'tx TransactionState>,

    /// The block head we're currently on.
    current: Option<ID>,

    /// The state of applicable attributes accumulated at up to the `current` block.
    current_attrs: Attrs,
    old_attrs: Attrs,

    /// The delta that's under construction.
    delta: Option<Delta<Out>>,
    /// Attributes for the `delta` that's under construction.
    attrs: Option<Box<Attrs>>,

    pending_delta: Option<Delta<Out>>,
}

impl<'tx> Uncommitted<'tx> {
    fn new(current: Option<ID>, tx: TxScope<'tx>, tx_state: Option<&'tx TransactionState>) -> Self {
        Uncommitted {
            tx,
            tx_state,
            current,
            current_attrs: Attrs::default(),
            old_attrs: Attrs::default(),
            delta: None,
            attrs: None,
            pending_delta: None,
        }
    }

    fn add_op(&mut self) -> Option<Delta<Out>> {
        let delta = self.delta.take()?;
        Some(match &delta {
            Delta::Insert(_, _) if !self.current_attrs.is_empty() => {
                delta.with_attrs(Some(Box::new(self.current_attrs.clone())))
            }
            Delta::Retain(_, _) => {
                if let Some(attrs) = &self.attrs
                    && !attrs.is_empty()
                {
                    delta.with_attrs(Some(attrs.clone()))
                } else {
                    delta
                }
            }
            _ => delta,
        })
    }

    fn finish(&mut self) -> Option<Delta<Out>> {
        match self.delta.take()? {
            Delta::Retain(_, None) => None,
            other => Some(other.with_attrs(self.attrs.take())),
        }
    }

    fn update_attrs(&mut self, key: &str, value: lib0::Value) {
        if value.is_null() {
            self.current_attrs.remove(key);
        } else {
            self.current_attrs.insert(key.to_string(), value);
        }
    }

    fn move_next(&mut self) -> crate::Result<Option<Delta<Out>>> {
        let state = match self.tx_state {
            Some(state) => state,
            None => return Ok(None),
        };

        if let Some(delta) = self.pending_delta.take() {
            return Ok(Some(delta));
        }

        let contents = self.tx.db.contents();
        while let Some(id) = self.current.take() {
            let block = self.tx.cursor.seek(id)?;
            self.current = block.right().copied();

            let mut delta = None;
            match block.content_type() {
                ContentType::String => {
                    if state.has_added(&id) {
                        if !state.has_deleted(&id) {
                            let content = get_content(&block, &contents)?;
                            let str = content.as_str()?;

                            if !matches!(self.delta, Some(Delta::Insert(_, _))) {
                                delta = self.add_op();
                                self.delta = Some(Delta::Insert(Out::Value(str.into()), None));
                            } else if let Some(Delta::Insert(Out::Value(Value::String(buf)), _)) =
                                &mut self.delta
                            {
                                buf.push_str(str);
                            } else {
                                unreachable!()
                            }
                        }
                    } else if state.has_deleted(&id) {
                        let block_len = block.clock_len().get() as usize;
                        if !matches!(self.delta, Some(Delta::Delete(_))) {
                            delta = self.add_op();
                            self.delta = Some(Delta::Delete(block_len));
                        } else if let Some(Delta::Delete(len)) = &mut self.delta {
                            *len += block_len;
                        } else {
                            unreachable!()
                        }
                    } else if !block.is_deleted() {
                        let block_len = block.clock_len().get() as usize;
                        if !matches!(self.delta, Some(Delta::Retain(_, _))) {
                            delta = self.add_op();
                            self.delta = Some(Delta::Retain(block_len, None));
                        } else if let Some(Delta::Retain(len, _)) = &mut self.delta {
                            *len += block_len;
                        } else {
                            unreachable!()
                        }
                    }
                }
                ContentType::Format => {
                    let content = get_content(&block, &contents)?;
                    let fmt = content.as_format()?;
                    let key = fmt.key();
                    let value = fmt.value()?;

                    if state.has_added(&id) {
                        if !state.has_deleted(&id) {
                            let current_value = self.current_attrs.get(key);
                            if current_value != Some(&value) {
                                if !matches!(self.delta, Some(Delta::Retain(_, _))) {
                                    delta = self.add_op();
                                }
                                match self.old_attrs.get(key) {
                                    None if value == Value::Null => {
                                        if let Some(attrs) = &mut self.attrs {
                                            attrs.remove(key);
                                        }
                                    }
                                    Some(old_value) if &value == old_value => {
                                        if let Some(attrs) = &mut self.attrs {
                                            attrs.remove(key);
                                        }
                                    }
                                    _ => {
                                        let attrs = self.attrs.get_or_insert_default();
                                        attrs.insert(key.into(), value);
                                    }
                                }
                            } else {
                                // ??
                            }
                        }
                    } else if state.has_deleted(&id) {
                        self.old_attrs.insert(key.into(), value.clone());
                        let current_value = self.current_attrs.get(key).unwrap_or(&Value::Null);
                        if current_value != &value {
                            let current_value = current_value.clone();
                            if matches!(self.delta, Some(Delta::Retain(_, _))) {
                                delta = self.add_op();
                            }
                            let attrs = self.attrs.get_or_insert_default();
                            attrs.insert(key.into(), current_value);
                        }
                    } else if !block.is_deleted() {
                        self.old_attrs.insert(key.into(), value.clone());
                        if let Some(attrs) = &mut self.attrs {
                            if let Some(attr) = attrs.get(key) {
                                if attr != &value {
                                    if matches!(self.delta, Some(Delta::Retain(_, _))) {
                                        // same as self.add_op() but without encapsulation that breaks borrow checker
                                        delta = match self.delta.take() {
                                            Some(delta) if !self.current_attrs.is_empty() => {
                                                Some(delta.with_attrs(Some(Box::new(
                                                    self.current_attrs.clone(),
                                                ))))
                                            }
                                            delta => delta,
                                        };
                                    }
                                    if value == Value::Null {
                                        attrs.remove(key);
                                    } else {
                                        attrs.insert(key.into(), value);
                                    }
                                } else {
                                    // ??
                                }
                            }
                        }
                    }

                    if !block.is_deleted() {
                        let delta = if matches!(self.delta, Some(Delta::Insert(_, _))) {
                            self.add_op()
                        } else {
                            None
                        };

                        self.update_attrs(fmt.key(), fmt.value()?);
                        if delta.is_some() {
                            return Ok(delta);
                        }
                    }
                }
                ContentType::Embed | ContentType::Node => {
                    if state.has_added(&id) {
                        if !state.has_deleted(&id) {
                            delta = self.add_op();
                            let out = match block.content_type() {
                                ContentType::Embed => {
                                    let content = get_content(&block, &contents)?;
                                    Out::Value(content.as_embed()?)
                                }
                                ContentType::Node => Out::Node(*block.id()),
                                _ => unreachable!(),
                            };
                            self.pending_delta = Some(Delta::Insert(out, None));
                        }
                    } else if state.has_deleted(&id) {
                        if !matches!(self.delta, Some(Delta::Delete(_))) {
                            delta = self.add_op();
                            self.delta = Some(Delta::Delete(1));
                        } else if let Some(Delta::Delete(len)) = &mut self.delta {
                            *len += 1;
                        } else {
                            unreachable!()
                        }
                    } else if !block.is_deleted() {
                        if !matches!(self.delta, Some(Delta::Retain(_, _))) {
                            delta = self.add_op();
                            self.delta = Some(Delta::Retain(1, None));
                        } else if let Some(Delta::Retain(len, _)) = &mut self.delta {
                            *len += 1;
                        } else {
                            unreachable!()
                        }
                    }
                }
                _ => { /* ignore */ }
            }

            if delta.is_some() {
                return Ok(delta);
            }
        }
        Ok(self.finish())
    }
}

impl<'tx> Iterator for Uncommitted<'tx> {
    type Item = crate::Result<Delta<Out>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.move_next() {
            Ok(Some(delta)) => Some(Ok(delta)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

pub struct Chunks<'a, 'tx> {
    tx: TxScope<'tx>,
    current: Option<ID>,
    from: Option<&'a Snapshot>,
    to: Option<&'a Snapshot>,

    buf: String,
    current_attrs: Option<Box<Attrs>>,
    current_op: Option<Op>,
    pending: Option<Chunk>,
}

impl<'a, 'tx> Chunks<'a, 'tx> {
    fn new(
        tx: TxScope<'tx>,
        start: Option<ID>,
        from: Option<&'a Snapshot>,
        to: Option<&'a Snapshot>,
    ) -> Self {
        Chunks {
            tx,
            current: start,
            from,
            to,
            buf: String::new(),
            current_attrs: None,
            current_op: None,
            pending: None,
        }
    }

    fn pack_str(&mut self) -> Option<Chunk> {
        if !self.buf.is_empty() {
            let attributes = match &self.current_attrs {
                Some(attrs) if attrs.is_empty() => None,
                other => other.clone(),
            };
            let mut buf = std::mem::replace(&mut self.buf, String::new());
            buf.shrink_to_fit();
            Some(Chunk {
                insert: Out::Value(buf.into()),
                attributes,
                operation: self.current_op.take(),
            })
        } else {
            None
        }
    }

    fn seen(snapshot: Option<&Snapshot>, block: &Block<'_>) -> bool {
        if let Some(s) = snapshot {
            s.is_visible(block.id())
        } else {
            !block.is_deleted()
        }
    }

    fn update_attrs(&mut self, key: &str, value: lib0::Value) {
        let attrs = self.current_attrs.get_or_insert_default();
        if value.is_null() {
            attrs.remove(key);
        } else {
            attrs.insert(key.to_string(), value);
        }
    }

    fn stash_or_return(&mut self, out: Out) -> Chunk {
        let attributes = match &self.current_attrs {
            Some(attrs) if attrs.is_empty() => None,
            attrs => attrs.clone(),
        };
        if let Some(chunk) = self.pack_str() {
            // There was already a string chunk that we were collecting, we need to
            // emit it first. Therefore, we store this chunk for the next method call
            self.pending = Some(Chunk {
                insert: out,
                attributes,
                operation: None,
            });
            chunk
        } else {
            Chunk {
                insert: out,
                attributes,
                operation: None,
            }
        }
    }

    fn move_next(&mut self) -> crate::Result<Option<Chunk>> {
        if let Some(chunk) = self.pending.take() {
            // in some cases like `stash_or_return` we might get 2 chunks to emit, but this method
            // only emits one at the time, so we might have a pending chunk from the previous step
            return Ok(Some(chunk));
        }

        while let Some(id) = self.current.take() {
            let block = self.tx.cursor.seek(id)?;
            self.current = block.right().copied();

            // check if block is within the bounds we're looking after
            if Self::seen(self.to, &block) || (self.from.is_some() && Self::seen(self.from, &block))
            {
                match block.content_type() {
                    ContentType::String => {
                        let mut prev = None;
                        if let Some(snapshot) = self.to {
                            if !snapshot.is_visible(&id) {
                                prev = self.pack_str();
                                self.current_op = Some(Op::Delete(id));
                            } else if let Some(snapshot) = self.from {
                                if !snapshot.is_visible(&id) {
                                    prev = self.pack_str();
                                    self.current_op = Some(Op::Insert(id));
                                } else if self.current_op.is_some() {
                                    prev = self.pack_str();
                                }
                            };
                        };
                        let contents = self.tx.db.contents();
                        let content = get_content(&block, &contents)?;
                        let str = content.as_str()?;
                        self.buf.push_str(str);
                        if prev.is_some() {
                            return Ok(prev);
                        }
                    }
                    ContentType::Embed => {
                        let contents = self.tx.db.contents();
                        let content = get_content(&block, &contents)?;
                        let out: Out = Out::Value(content.as_embed()?);
                        return Ok(Some(self.stash_or_return(out)));
                    }
                    ContentType::Node => {
                        let out: Out = Out::Node(*block.id());
                        return Ok(Some(self.stash_or_return(out)));
                    }
                    ContentType::Format => {
                        if Self::seen(self.to, &block) {
                            let chunk = self.pack_str();
                            let contents = self.tx.db.contents();
                            let content = get_content(&block, &contents)?;
                            let fmt = content.as_format()?;
                            self.update_attrs(fmt.key(), fmt.value()?);

                            if let Some(chunk) = chunk {
                                return Ok(Some(chunk));
                            }
                        }
                    }
                    _ => { /* ignore */ }
                }
            }
        }

        // this is always (potentially) the last block
        Ok(self.pack_str())
    }
}

impl<'a, 'tx> Iterator for Chunks<'a, 'tx> {
    type Item = crate::Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.move_next() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl Delta<In> {
    pub fn retain(len: usize) -> Self {
        Delta::Retain(len, None)
    }

    pub fn insert<T: Into<In>>(value: T) -> Self {
        Delta::Insert(value.into(), None)
    }

    pub fn insert_with<T: Into<In>>(value: T, attrs: Attrs) -> Self {
        Delta::Insert(value.into(), Some(Box::new(attrs)))
    }

    pub fn delete(len: usize) -> Self {
        Delta::Delete(len)
    }
}

struct BlockPosition<'a> {
    parent: &'a mut BlockMut,
    attrs: Attrs,
    utf16_index: usize,
    left: Option<ID>,
    right: Option<ID>,
}

impl<'a> BlockPosition<'a> {
    fn new(parent: &'a mut BlockMut) -> Self {
        let right = parent.start().copied();
        BlockPosition {
            parent,
            attrs: Attrs::new(),
            utf16_index: 0,
            left: None,
            right,
        }
    }

    fn forward_by(&mut self, offset: usize, cursor: &mut BlockCursor) -> crate::Result<()> {
        let mut remaining = offset;
        while let Some(right_id) = &self.right
            && remaining != 0
        {
            let right = cursor.seek(*right_id)?;
            if !right.is_deleted() {
                if right.content_type() == ContentType::Format {
                    let content_store = cursor.content_store();
                    let content = get_content(&right, &content_store)?;
                    let fmt = content.as_format()?;
                    let fmt_value: Value = fmt.value()?;
                    if fmt_value.is_null() {
                        self.attrs.remove(fmt.key());
                    } else {
                        self.attrs.insert(fmt.key().to_owned(), fmt_value);
                    }
                } else {
                    let len = right.clock_len().get() as usize;
                    if remaining < len {
                        // Actually split the block in the store so that downstream
                        // consumers (insert_at, remove_at, ...) can address `pos.right`
                        // as a real block boundary. After the split, `pos.left` is the
                        // last id of the left portion and `pos.right` is the first id
                        // of the right portion.
                        let split_id = ID::new(
                            right_id.client,
                            right_id.clock + Clock::new(remaining as u32),
                        );
                        // Drop the borrow on `block_cursor` before opening another cursor
                        // through `blocks.split`.
                        let _ = right;
                        match cursor.split(split_id)? {
                            SplitResult::Split(left_block, right_block) => {
                                self.left = Some(left_block.last_id());
                                self.right = Some(*right_block.id());
                            }
                            SplitResult::Unchanged(_) => {
                                // Should not happen: we verified `remaining < len`, so
                                // the split point is strictly inside the block.
                                unreachable!("split point is strictly inside the block");
                            }
                        }
                        self.utf16_index += remaining;
                        break;
                    } else {
                        remaining -= len;
                        self.utf16_index += len;
                    }
                }
            }
            // move to the right
            self.left = Some(right.last_id());
            self.right = right.right().copied();
        }
        Ok(())
    }

    fn seek(
        cursor: &mut BlockCursor,
        parent: &'a mut BlockMut,
        utf16_index: usize,
    ) -> crate::Result<Self> {
        let mut pos = Self::new(parent);
        pos.forward_by(utf16_index, cursor)?;

        Ok(pos)
    }

    fn unset_missing(&mut self, attrs: &mut Attrs) {
        // if current `attributes` don't confirm the same keys as the formatting wrapping
        // current insert position, they should be unset
        for (k, _) in self.attrs.iter() {
            if !attrs.contains_key(k) {
                attrs.insert(k.clone(), Value::Null);
            }
        }
    }

    fn insert_internal<'tx, P: Prelim>(
        &mut self,
        tx: &mut TxMutScope<'tx>,
        value: P,
    ) -> crate::Result<P::Return> {
        let left = self.left.as_ref();
        let right = self.right.as_ref();

        let (block, result) =
            InsertBlockData::insert_block(tx, &mut self.parent, left, right, None, value)?;
        self.left = Some(block.last_id());
        Ok(result)
    }

    fn minimize(&mut self, attrs: &Attrs, cursor: &mut BlockCursor) -> crate::Result<()> {
        // go right while attrs[right.key] === right.value (or right is deleted)
        while let Some(right_id) = self.right {
            let right = cursor.seek(right_id)?;
            if right.is_deleted() {
                forward(self, cursor)?;
            } else {
                if right.content_type() == ContentType::Format {
                    let contents = cursor.content_store();
                    let content = get_content(&right, &contents)?;
                    let fmt = content.as_format()?;
                    if let Some(attr_value) = attrs.get(fmt.key()) {
                        if attr_value == &fmt.value()? {
                            forward(self, cursor)?;
                            continue;
                        }
                    }
                }
                break;
            }
        }
        Ok(())
    }

    fn insert_attributes<'tx>(
        &mut self,
        tx: &mut TxMutScope<'tx>,
        attrs: Box<Attrs>,
    ) -> crate::Result<Attrs> {
        let mut negated = Attrs::new();
        for (name, value) in attrs.into_iter() {
            let current_value = self.attrs.get(&name).unwrap_or(&Value::Null);
            if current_value != &value {
                // insert attribute
                let negated_value = current_value.clone();
                self.insert_internal(tx, FormatPrelim::new(&name, value))?;
                negated.insert(name, negated_value);
            }
        }

        Ok(negated)
    }

    fn insert_negated<'tx>(
        &mut self,
        tx: &mut TxMutScope<'tx>,
        mut attrs: Attrs,
    ) -> crate::Result<()> {
        // first cleanup the attributes that were already ended
        {
            while let Some(right_id) = self.right {
                let block = tx.cursor.seek(right_id)?;
                if block.is_deleted() {
                    forward(self, &mut tx.cursor)?;
                    continue;
                }
                if block.content_type() == ContentType::Format {
                    let contents = tx.db.contents();
                    let content = get_content(&block, &contents)?;
                    let fmt = content.as_format()?;
                    let key = fmt.key();
                    if let Some(curr_value) = attrs.get(key)
                        && curr_value == &fmt.value()?
                    {
                        attrs.remove(key);
                        forward(self, &mut tx.cursor)?;
                        continue;
                    }
                }
                break;
            }
        }

        // second add remaining attributes
        for (key, value) in attrs.iter() {
            let fmt = FormatPrelim::new(key, value);
            let (block, _) = InsertBlockData::insert_block(
                tx,
                &mut self.parent,
                self.left.as_ref(),
                self.right.as_ref(),
                None,
                fmt,
            )?;
            self.left = Some(block.last_id());
        }
        Ok(())
    }
}

fn forward(pos: &mut BlockPosition, cursor: &mut BlockCursor) -> crate::Result<bool> {
    if let Some(right) = pos.right.take() {
        let block = cursor.seek(right)?;
        if !block.is_deleted() {
            match block.content_type() {
                ContentType::String | ContentType::Embed => {
                    pos.utf16_index += block.clock_len().get() as usize;
                }
                ContentType::Format => {
                    let content_store = cursor.content_store();
                    let data = get_content(&block, &content_store)?;
                    let fmt = data.as_format()?;
                    let key = fmt.key();
                    let value: lib0::Value = fmt.value()?;
                    if value.is_null() {
                        pos.attrs.remove(key);
                    } else {
                        pos.attrs.insert(key.to_owned(), value);
                    }
                }
                _ => { /* ignore */ }
            }
        }

        pos.left = Some(block.last_id());
        pos.right = block.right().copied();
        Ok(true)
    } else {
        Ok(false)
    }
}

fn clean_format_gap<'tx>(
    tx: &mut TxMutScope<'tx>,
    start: &ID,
    end: Option<&ID>,
    start_attrs: &Attrs,
    end_attrs: &mut Attrs,
) -> crate::Result<usize> {
    let mut end = end.copied();
    while let Some(end_id) = end {
        let block = tx.cursor.seek(end_id)?;
        match block.content_type() {
            ContentType::String | ContentType::Embed => break,
            ContentType::Format if !block.is_deleted() => {
                let contents = tx.db.contents();
                let content = get_content(&block, &contents)?;
                let fmt = content.as_format()?;
                let key = fmt.key();
                let value: lib0::Value = fmt.value()?;
                if value.is_null() {
                    end_attrs.remove(key);
                } else {
                    end_attrs.insert(key.to_owned(), value);
                }
            }
            _ => { /* ignore */ }
        }
        end = block.right().copied();
    }

    let mut cleanups = 0;
    let mut current = Some(*start);
    while let Some(current_id) = current
        && end != current
    {
        let block = tx.cursor.seek(current_id)?;
        if !block.is_deleted() {
            if block.content_type() == ContentType::Format {
                let contents = tx.db.contents();
                let content = get_content(&block, &contents)?;
                let fmt = content.as_format()?;
                let key = fmt.key();
                let value: lib0::Value = fmt.value()?;
                let e = end_attrs.get(key).unwrap_or(&Value::Null);
                let s = start_attrs.get(key).unwrap_or(&Value::Null);
                if e != &value || s == &value {
                    tx.delete(&mut block.into(), false)?;
                    cleanups += 1;
                }
            }
        }
        current = block.right().copied();
    }
    Ok(cleanups)
}

fn get_content<'a>(block: &Block<'a>, contents: &'a ContentStore) -> crate::Result<Content<'a>> {
    match block.try_inline_content() {
        Some(content) => Ok(content),
        None => {
            let data = contents.get(*block.id())?;
            Ok(Content::new(block.content_type(), Cow::Borrowed(data)))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::block::ID;
    use crate::lib0::Value;
    use crate::read::{Decode, DecoderV1};
    use crate::test_util::{multi_doc, sync};
    use crate::types::text::{Attrs, Chunk, Delta, Op};
    use crate::write::Encode;
    use crate::{ListPrelim, Map, MapPrelim, Out, StateVector, Text, Unmounted, lib0};

    #[test]
    fn insert_empty_string() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        assert_eq!(txt.to_string(), "");

        txt.push("").unwrap();
        assert_eq!(txt.to_string(), "");

        txt.push("abc").unwrap();
        txt.push("").unwrap();
        assert_eq!(txt.to_string(), "abc");

        tx.commit(None).unwrap();
    }

    #[test]
    fn append_single_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "a").unwrap();
        txt.insert(1, "b").unwrap();
        txt.insert(2, "c").unwrap();

        assert_eq!(txt.to_string(), "abc");

        tx.commit(None).unwrap();
    }

    #[test]
    fn append_mutli_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello").unwrap();
        txt.insert(5, " ").unwrap();
        txt.insert(6, "world").unwrap();

        assert_eq!(txt.to_string(), "hello world");

        tx.commit(None).unwrap();
    }

    #[test]
    fn prepend_single_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "a").unwrap();
        txt.insert(0, "b").unwrap();
        txt.insert(0, "c").unwrap();

        assert_eq!(txt.to_string(), "cba");

        tx.commit(None).unwrap();
    }

    #[test]
    fn prepend_mutli_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello").unwrap();
        txt.insert(0, " ").unwrap();
        txt.insert(0, "world").unwrap();

        assert_eq!(txt.to_string(), "world hello");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_after_block() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello").unwrap();
        txt.insert(5, " ").unwrap();
        txt.insert(6, "world").unwrap();
        txt.insert(6, "beautiful ").unwrap();

        assert_eq!(txt.to_string(), "hello beautiful world");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_inside_of_block() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "it was expected").unwrap();
        txt.insert(6, " not").unwrap();

        assert_eq!(txt.to_string(), "it was not expected");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_concurrent_root() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "hello ").unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut txt2 = txt.mount_mut(&mut t2).unwrap();

        txt2.insert(0, "world").unwrap();

        drop(txt1);
        drop(txt2);

        let d1_sv = t1.state_vector().unwrap().encode().unwrap();
        let d2_sv = t2.state_vector().unwrap().encode().unwrap();

        let u1 = t1
            .diff_update(&StateVector::decode(&d2_sv).unwrap())
            .unwrap();
        let u2 = t2
            .diff_update(&StateVector::decode(&d1_sv).unwrap())
            .unwrap();

        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let a = txt.mount(&t1).unwrap().to_string();
        let b = txt.mount(&t2).unwrap().to_string();

        assert_eq!(a, b);
        assert_eq!(a.as_str(), "hello world");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn insert_concurrent_in_the_middle() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "I expect that").unwrap();
        assert_eq!(txt1.to_string(), "I expect that");

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        drop(txt1);

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .diff_update(&StateVector::decode(&d2_sv).unwrap())
            .unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "I expect that");

        txt2.insert(1, " have").unwrap();
        txt2.insert(13, "ed").unwrap();
        assert_eq!(txt2.to_string(), "I have expected that");

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(1, " didn't").unwrap();
        assert_eq!(txt1.to_string(), "I didn't expect that");

        drop(txt1);
        drop(txt2);

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let d1_sv = t1.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .diff_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        let u2 = t2
            .diff_update(&StateVector::decode(&d1_sv.as_slice()).unwrap())
            .unwrap();
        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();

        let a = txt1.to_string();
        let b = txt2.to_string();

        assert_eq!(a, b);
        assert_eq!(a.as_str(), "I didn't have expected that");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn append_concurrent() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "aaa").unwrap();
        assert_eq!(txt1.to_string(), "aaa");

        drop(txt1);

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .diff_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "aaa");

        txt2.insert(3, "bbb").unwrap();
        txt2.insert(6, "bbb").unwrap();
        assert_eq!(txt2.to_string(), "aaabbbbbb");

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(3, "aaa").unwrap();
        assert_eq!(txt1.to_string(), "aaaaaa");

        drop(txt1);
        drop(txt2);

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let d1_sv = t1.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .diff_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        let u2 = t2
            .diff_update(&StateVector::decode(&d1_sv.as_slice()).unwrap())
            .unwrap();

        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();

        let a = txt1.to_string();
        let b = txt2.to_string();

        assert_eq!(a.as_str(), "aaaaaabbbbbb");
        assert_eq!(a, b);

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn delete_single_block_start() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "bbb").unwrap();
        txt.insert(0, "aaa").unwrap();
        txt.remove_range(0..3).unwrap();

        assert_eq!(txt.len(), 3);
        assert_eq!(txt.to_string(), "bbb");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_single_block_end() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "bbb").unwrap();
        txt.insert(0, "aaa").unwrap();
        txt.remove_range(3..6).unwrap();

        assert_eq!(txt.to_string(), "aaa");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_multiple_whole_blocks() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "a").unwrap();
        txt.insert(1, "b").unwrap();
        txt.insert(2, "c").unwrap();

        txt.remove_range(1..=1).unwrap();
        assert_eq!(txt.to_string(), "ac");

        txt.remove_range(1..=1).unwrap();
        assert_eq!(txt.to_string(), "a");

        txt.remove_range(0..1).unwrap();
        assert_eq!(txt.to_string(), "");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_slice_of_block() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "abc").unwrap();
        txt.remove_range(1..=1).unwrap();

        assert_eq!(txt.to_string(), "ac");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_multiple_blocks_with_slicing() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello ").unwrap();
        txt.insert(6, "beautiful").unwrap();
        txt.insert(15, " world").unwrap();

        txt.remove_range(5..16).unwrap();
        assert_eq!(txt.to_string(), "helloworld");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_after_delete() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello ").unwrap();
        txt.remove_range(0..5).unwrap();
        txt.insert(1, "world").unwrap();

        assert_eq!(txt.to_string(), " world");

        tx.commit(None).unwrap();
    }

    #[test]
    fn concurrent_insert_delete() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "hello world").unwrap();
        assert_eq!(txt1.to_string(), "hello world");

        drop(txt1);

        let u1 = t1.diff_update(&StateVector::default()).unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "hello world");

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(5, " beautiful").unwrap();
        txt1.insert(21, "!").unwrap();
        txt1.remove_range(0..5).unwrap();
        assert_eq!(txt1.to_string(), " beautiful world!");

        txt2.remove_range(5..10).unwrap();
        txt2.remove_range(0..1).unwrap();
        txt2.insert(0, "H").unwrap();
        assert_eq!(txt2.to_string(), "Hellod");

        drop(txt1);
        drop(txt2);

        let sv1 = t1.state_vector().unwrap().encode().unwrap();
        let sv2 = t2.state_vector().unwrap().encode().unwrap();
        let u1 = t1.diff_update(&StateVector::decode(&sv2).unwrap()).unwrap();
        let u2 = t2.diff_update(&StateVector::decode(&sv1).unwrap()).unwrap();

        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();
        let a = txt1.to_string();
        let b = txt2.to_string();

        assert_eq!(a, b);
        assert_eq!(a, "H beautifuld!".to_owned());
    }

    #[test]
    fn basic_format() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let a = Attrs::from([("bold".into(), Value::Bool(true))]);

        // step 1
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.insert_with(0, "abc", a.clone()).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Insert("abc".into(), Some(Box::new(a.clone())))];

            assert_eq!(txt1.to_string(), "abc".to_string());
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("abc").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();

            assert_eq!(txt2.to_string(), "abc");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 2
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.remove_range(0..1).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();
            let expected = vec![Delta::Delete(1)];

            assert_eq!(txt1.to_string(), "bc");
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("bc").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();

            assert_eq!(txt2.to_string(), "bc");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 3
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.remove_range(1..2).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Retain(1, None), Delta::Delete(1)];

            assert_eq!(txt1.to_string(), "b");
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("b").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);

            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();

            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "b");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 4
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.insert_with(0, "z", a.clone()).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Insert(Out::from("z"), Some(Box::new(a.clone())))];

            assert_eq!(txt1.to_string(), "zb".to_string());
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("zb").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "zb");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 5
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.insert(0, "y").unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Insert("y".into(), None)];

            assert_eq!(txt1.to_string(), "yzb".to_string());
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("y"), Chunk::new("zb").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();

            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "yzb");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 6
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            let b = Attrs::from([("bold".into(), Value::Null)]);
            txt1.format(0..2, b.clone()).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Retain(1, None), Delta::Retain(1, Some(Box::new(b)))];

            assert_eq!(txt1.to_string(), "yzb");
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("yz"), Chunk::new("b").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "yzb");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }
    }

    #[test]
    fn embed_with_attributes() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);

        let a1 = Attrs::from([("bold".into(), true.into())]);
        let embed = lib0!({
            "image": "imageSrc.png"
        });

        let update_v1 = {
            let mut t1 = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut t1).unwrap();

            txt1.insert_with(0, "ab", a1.clone()).unwrap();

            let a2 = Attrs::from([("width".into(), Value::from(100.0))]);

            txt1.insert_embed_with(1, embed.clone(), a2.clone())
                .unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![
                Delta::Insert("a".into(), Some(Box::new(a1.clone()))),
                Delta::Insert(embed.clone().into(), Some(Box::new(a2.clone()))),
                Delta::Insert("b".into(), Some(Box::new(a1.clone()))),
            ];
            assert_eq!(uncommitted, expected);
            t1.commit(None).unwrap();

            let expected = vec![
                Chunk::new("a").with_attrs(a1.clone()),
                Chunk::new(embed.clone()).with_attrs(a2),
                Chunk::new("b").with_attrs(a1.clone()),
            ];
            let t1 = d1.transact_mut("test").unwrap();
            let txt1 = txt.mount(&t1).unwrap();
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                expected
            );

            let update_v1 = t1.diff_update(&StateVector::default()).unwrap();
            update_v1
        };

        let a2 = Attrs::from([("width".into(), Value::from(100.0))]);

        let expected = vec![
            Chunk::new("a").with_attrs(a1.clone()),
            Chunk::new(embed).with_attrs(a2),
            Chunk::new("b").with_attrs(a1),
        ];

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&update_v1))
            .unwrap();
        let txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(
            txt2.chunks().map(Result::unwrap).collect::<Vec<_>>(),
            expected
        );
        t2.commit(None).unwrap();
    }

    #[test]
    fn issue_101() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        let attrs = Attrs::from([("bold".into(), true.into())]);

        txt1.insert(0, "abcd").unwrap();
        t1.commit(None).unwrap();

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.format(1..3, attrs.clone()).unwrap();

        let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();
        let expected = vec![
            Delta::Retain(1, None),
            Delta::Retain(2, Some(Box::new(attrs))),
        ];
        assert_eq!(uncommitted, expected);
        t1.commit(None).unwrap();
    }

    #[test]
    fn text_diff_adjacent() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        let attrs1 = Attrs::from_iter([("a".to_string(), Value::from("a"))]);
        txt.insert_with(0, "abc", attrs1.clone()).unwrap();
        let attrs2 = Attrs::from_iter([
            ("a".to_string(), Value::from("a")),
            ("b".into(), "b".into()),
        ]);
        txt.insert_with(3, "def", attrs2.clone()).unwrap();

        let diff: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        let expected = vec![
            Chunk::new("abc").with_attrs(attrs1),
            Chunk::new("def").with_attrs(attrs2),
        ];
        assert_eq!(diff, expected);

        txn.commit(None).unwrap();
    }

    #[test]
    fn text_remove_4_byte_range() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "😭😊").unwrap();

        sync([&mut t1, &mut t2]);

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.remove_range(0.."😭".encode_utf16().count()).unwrap();
        assert_eq!(txt1.to_string(), "😊");

        sync([&mut t1, &mut t2]);
        let txt2 = txt.mount(&t2).unwrap();
        assert_eq!(txt2.to_string(), "😊");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn text_remove_3_byte_range() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(0, "⏰⏳").unwrap();

        sync([&mut t1, &mut t2]);

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.remove_range(0.."⏰".encode_utf16().count()).unwrap();
        assert_eq!(txt1.to_string(), "⏳");

        sync([&mut t1, &mut t2]);
        let txt2 = txt.mount(&t1).unwrap();
        assert_eq!(txt2.to_string(), "⏳");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn delete_4_byte_character_from_middle() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "😊😭").unwrap();
        let start = "😊".encode_utf16().count();
        let end = start + "😭".encode_utf16().count();
        txt.remove_range(start..end).unwrap();

        assert_eq!(txt.to_string(), "😊");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_3_byte_character_from_middle_1() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "⏰⏳").unwrap();
        let start = "⏰".encode_utf16().count();
        let end = start + "⏳".encode_utf16().count();
        txt.remove_range(start..end).unwrap();

        assert_eq!(txt.to_string(), "⏰");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_3_byte_character_from_middle_2() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "👯🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨").unwrap();

        let start = "👯".encode_utf16().count();
        let end = start + "🙇‍♀️🙇‍♀️".encode_utf16().count();
        txt.format(start..end, Attrs::default()).unwrap();
        let start = "👯🙇‍♀️🙇‍♀️".encode_utf16().count();
        let end = start + "⏰".encode_utf16().count();
        txt.remove_range(start..end).unwrap(); // will delete ⏰ and 👩‍❤️‍💋‍👨

        assert_eq!(txt.to_string(), "👯🙇‍♀️🙇‍♀️👩‍❤️‍💋‍👨");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_3_byte_character_from_middle_after_insert_and_format() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨").unwrap();
        txt.insert(0, "👯").unwrap();
        let start = "👯".encode_utf16().count();
        let end = start + "🙇‍♀️🙇‍♀️".encode_utf16().count();
        txt.format(start..end, Attrs::default()).unwrap();

        // will delete ⏰ and 👩‍❤️‍💋‍👨
        let start = "👯🙇‍♀️🙇‍♀️".encode_utf16().count();
        let end = start + "⏰".encode_utf16().count();
        txt.remove_range(start..end).unwrap(); // will delete ⏰ and 👩‍❤️‍💋‍👨

        assert_eq!(&txt.to_string(), "👯🙇‍♀️🙇‍♀️👩‍❤️‍💋‍👨");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_multi_byte_character_from_middle_after_insert_and_format() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "❤️❤️🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨👩‍❤️‍💋‍👨").unwrap();
        txt.insert(0, "👯").unwrap();
        let start = "👯".encode_utf16().count();
        let end = start + "❤️❤️🙇‍♀️🙇‍♀️⏰".encode_utf16().count();
        txt.format(start..end, Attrs::new()).unwrap();
        txt.insert("👯❤️❤️🙇‍♀️🙇‍♀️⏰".encode_utf16().count(), "⏰")
            .unwrap();
        let start = "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰".encode_utf16().count();
        let end = start + "👩‍❤️‍💋‍👨".encode_utf16().count();
        txt.format(start..end, Attrs::new()).unwrap();
        let start = "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰👩‍❤️‍💋‍👩".encode_utf16().count();
        let end = start + "👩‍❤️‍💋‍👨".encode_utf16().count();
        txt.remove_range(start..end).unwrap();
        assert_eq!(txt.to_string(), "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰👩‍❤️‍💋‍👨");

        txn.commit(None).unwrap();
    }

    #[test]
    fn insert_string_with_no_attribute() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        let attrs = Attrs::from([("a".into(), "a".into())]);
        txt.insert_with(0, "ac", attrs.clone()).unwrap();
        txt.insert_with(1, "b", Attrs::new()).unwrap();

        let expect = vec![
            Chunk::new("a").with_attrs(attrs.clone()),
            Chunk::new("b"),
            Chunk::new("c").with_attrs(attrs.clone()),
        ];

        let actual: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(actual, expect);
        txn.commit(None).unwrap();
    }

    #[test]
    fn insert_empty_string_with_attributes() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let attrs = [("a".to_string(), Value::from("a"))];
        txt.insert(0, "abc").unwrap();
        txt.insert(1, "").unwrap(); // nothing changes
        txt.insert_with(1, "", attrs).unwrap(); // nothing changes

        assert_eq!(txt.to_string(), "abc");

        let bin = txn.diff_update(&StateVector::default()).unwrap();

        txn.commit(None).unwrap();

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();

        txn.apply_update(&mut DecoderV1::from_slice(&bin)).unwrap();

        let txt = root.mount(&txn).unwrap();
        assert_eq!(txt.to_string(), "abc");

        txn.commit(None).unwrap();
    }

    #[test]
    fn snapshots() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut text = txt.mount_mut(&mut txn).unwrap();

        text.insert(0, "hello").unwrap();
        let prev = txn.snapshot_uncommitted().unwrap();
        let mut text = txt.mount_mut(&mut txn).unwrap();
        text.insert(5, " world").unwrap();
        let next = txn.snapshot_uncommitted().unwrap();
        let text = txt.mount(&txn).unwrap();
        let diff: Vec<_> = text
            .chunks_between(Some(&prev), Some(&next))
            .map(Result::unwrap)
            .collect();

        assert_eq!(
            diff,
            vec![
                Chunk::new("hello"),
                Chunk::new(" world").with_op(Op::Insert(ID::new(1.into(), 5.into())))
            ]
        );
        txn.commit(None).unwrap();
    }

    #[test]
    fn diff_with_embedded_items() {
        let txt: Unmounted<Text> = Unmounted::root("article");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut text = txt.mount_mut(&mut txn).unwrap();

        let bold = Attrs::from_iter([("b".into(), true.into())]);
        let italic = Attrs::from_iter([("i".into(), true.into())]);

        text.insert_with(0, "hello world", italic.clone()).unwrap(); // "<i>hello world</i>"
        text.format(6..11, bold.clone()).unwrap(); // "<i>hello <b>world</b></i>"
        let image = vec![0, 0, 0, 0];
        text.insert_embed(5, Value::from(image.clone())).unwrap(); // insert binary after "hello"
        let array = text.insert_embed(5, ListPrelim::default()).unwrap(); // insert array ref after "hello"

        let italic_and_bold = Attrs::from([("b".into(), true.into()), ("i".into(), true.into())]);
        let chunks: Vec<_> = text.chunks().map(Result::unwrap).collect();
        assert_eq!(
            chunks,
            vec![
                Chunk::new("hello").with_attrs(italic.clone()),
                Chunk::new(array).with_attrs(italic.clone()),
                Chunk::new(image).with_attrs(italic.clone()),
                Chunk::new(" ").with_attrs(italic),
                Chunk::new("world").with_attrs(italic_and_bold),
            ]
        );
    }

    #[test]
    fn multiline_format() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let bold = Attrs::from_iter([("bold".into(), true.into())]);
        txt.insert(0, "Test\nMulti-line\nFormatting").unwrap();
        txt.apply_delta([
            Delta::Retain(4, Some(Box::new(bold.clone()))),
            Delta::retain(1), // newline character
            Delta::Retain(10, Some(Box::new(bold.clone()))),
            Delta::retain(1), // newline character
            Delta::Retain(10, Some(Box::new(bold.clone()))),
        ])
        .unwrap();
        let delta: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(
            delta,
            vec![
                Chunk::new("Test").with_attrs(bold.clone()),
                Chunk::new("\n"),
                Chunk::new("Multi-line").with_attrs(bold.clone()),
                Chunk::new("\n"),
                Chunk::new("Formatting").with_attrs(bold),
            ]
        );

        txn.commit(None).unwrap();
    }

    #[test]
    fn delta_with_embeds() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let linebreak = lib0!({
            "linebreak": "s"
        });
        txt.apply_delta([Delta::insert(linebreak.clone())]).unwrap();
        let delta: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(delta, vec![Chunk::new(linebreak)]);
    }

    #[test]
    fn delta_with_shared_ref() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut txt1 = root.mount_mut(&mut t1).unwrap();

        txt1.apply_delta([Delta::insert(MapPrelim::from_iter([(
            "key".into(),
            "val".into(),
        )]))])
        .unwrap();
        let delta: Vec<_> = txt1.chunks().map(Result::unwrap).collect();
        let node = delta[0].insert.as_node().cloned().unwrap();
        let map: Unmounted<Map> = Unmounted::nested(node);
        let map = map.mount(&t1).unwrap();
        let actual: Value = map.get("key").unwrap();
        assert_eq!(actual, Value::from("val"));

        let update = t1.incremental_update().unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&update))
            .unwrap();
        t1.commit(None).unwrap();
        t2.commit(None).unwrap();

        let t2 = d2.transact_mut("test").unwrap();
        let txt2 = root.mount(&t2).unwrap();
        let delta: Vec<_> = txt2.chunks().map(Result::unwrap).collect();
        assert_eq!(delta.len(), 1);
        let node = delta[0].insert.clone().as_node().cloned().unwrap();
        let map: Unmounted<Map> = Unmounted::nested(node);
        let map = map.mount(&t2).unwrap();
        let actual: Value = map.get("key").unwrap();
        assert_eq!(actual, Value::from("val"));
    }

    #[test]
    fn delta_snapshots() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();

        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([Delta::insert("abcd")]).unwrap();
        let snapshot1 = txn.snapshot_uncommitted().unwrap(); // 'abcd'

        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([Delta::retain(1), Delta::insert("x"), Delta::delete(1)])
            .unwrap();
        let snapshot2 = txn.snapshot_uncommitted().unwrap(); // 'axcd'

        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([
            Delta::retain(2),   // ax^cd
            Delta::delete(1),   // ax^d
            Delta::insert("x"), // axx^d
            Delta::delete(1),   // axx^
        ])
        .unwrap();
        let state1: Vec<_> = txt
            .chunks_between(None, Some(&snapshot1))
            .map(Result::unwrap)
            .collect();
        assert_eq!(state1, vec![Chunk::new("abcd")]);
        let state2: Vec<_> = txt
            .chunks_between(None, Some(&snapshot2))
            .map(Result::unwrap)
            .collect();
        assert_eq!(state2, vec![Chunk::new("axcd")]);
        let state2_diff: Vec<_> = txt
            .chunks_between(Some(&snapshot1), Some(&snapshot2))
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            state2_diff,
            vec![
                Chunk::new("a"),
                Chunk::new("x").with_op(Op::Insert(ID::new(1.into(), 4.into()))),
                Chunk::new("b").with_op(Op::Delete(ID::new(1.into(), 1.into()))),
                Chunk::new("cd"),
            ]
        );
    }

    #[test]
    fn snapshot_delete_after() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        txt.apply_delta([Delta::insert("abcd")]).unwrap();
        let snapshot1 = txn.snapshot_uncommitted().unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([Delta::retain(4), Delta::insert("e")])
            .unwrap();
        let state1: Vec<_> = txt
            .chunks_between(None, Some(&snapshot1))
            .map(Result::unwrap)
            .collect();
        assert_eq!(state1, vec![Chunk::new("abcd")]);
    }

    #[test]
    fn empty_delta_chunks() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let delta = vec![
            Delta::insert("a"),
            Delta::Insert(
                "".into(),
                Some(Box::new(Attrs::from([("bold".into(), true.into())]))),
            ),
            Delta::insert("b"),
        ];

        txt.apply_delta(delta).unwrap();
        assert_eq!(txt.to_string(), "ab");

        let bin = txn.diff_update(&StateVector::default()).unwrap();

        txn.commit(None).unwrap();

        let (mdoc, _) = multi_doc(2);
        let mut txn = mdoc.transact_mut("test").unwrap();

        txn.apply_update(&mut DecoderV1::from_slice(&bin)).unwrap();

        let txt = root.mount(&txn).unwrap();
        assert_eq!(txt.to_string(), "ab");
    }
}
