//! Safe abstraction layer over `lmdb-master-sys` FFI bindings.
//!
//! This module provides safe Rust wrappers for LMDB operations including environment
//! management, transactions, database handles, cursors, and key-value operations.
//! Lifetimes enforce that cursors and data references don't outlive their transactions.

use lmdb_master_sys::*;
use std::ffi::{CStr, CString};
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::null_mut;
// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// LMDB error wrapping a raw error code from the C library.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub(crate) i32);

impl Error {
    pub const NOT_FOUND: Self = Self(MDB_NOTFOUND);
    pub const KEY_EXISTS: Self = Self(MDB_KEYEXIST);
    pub const MAP_FULL: Self = Self(MDB_MAP_FULL);
    pub const DBS_FULL: Self = Self(MDB_DBS_FULL);
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = unsafe { CStr::from_ptr(mdb_strerror(self.0)) };
        write!(f, "{}", msg.to_string_lossy())
    }
}

impl std::error::Error for Error {}

/// Convert a raw LMDB return code into a `Result`.
#[inline]
fn lmdb_result(code: i32) -> Result<(), Error> {
    if code == MDB_SUCCESS {
        Ok(())
    } else {
        Err(Error(code))
    }
}

// ---------------------------------------------------------------------------
// MDB_val helpers
// ---------------------------------------------------------------------------

#[inline]
fn to_mdb_val(bytes: &[u8]) -> MDB_val {
    MDB_val {
        mv_size: bytes.len(),
        mv_data: bytes.as_ptr() as *mut _,
    }
}

#[inline]
fn empty_mdb_val() -> MDB_val {
    MDB_val {
        mv_size: 0,
        mv_data: std::ptr::null_mut(),
    }
}

/// Interpret an `MDB_val` as a byte slice with the given lifetime.
///
/// # Safety
/// The caller must ensure the `MDB_val` points to valid memory that lives for `'a`.
#[inline]
unsafe fn from_mdb_val<'a>(val: &MDB_val) -> &'a [u8] {
    if val.mv_data.is_null() || val.mv_size == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(val.mv_data as *const u8, val.mv_size) }
    }
}

// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------

/// LMDB environment handle. Owns the underlying `MDB_env`.
pub struct Env {
    env: *mut MDB_env,
}

// LMDB environments are safe to share across threads.
unsafe impl Send for Env {}
unsafe impl Sync for Env {}

impl Env {
    /// Start building a new environment.
    pub fn builder() -> EnvBuilder {
        EnvBuilder::new()
    }

    /// Open (or create) a named database, returning its handle.
    ///
    /// This internally creates and commits a short-lived write transaction.
    /// Must not be called concurrently with other `create_db` calls.
    pub fn create_db(&self, name: &str, flags: u32) -> Result<Dbi, Error> {
        let txn = self.begin_rw_txn()?;
        let c_name = CString::new(name).expect("database name must not contain null bytes");
        let mut dbi: MDB_dbi = 0;
        let rc = unsafe { mdb_dbi_open(txn.txn, c_name.as_ptr(), flags, &mut dbi) };
        lmdb_result(rc)?;
        txn.commit()?;
        Ok(Dbi(dbi))
    }

    /// Begin a new read-write transaction.
    pub fn begin_rw_txn(&self) -> Result<RwTxn<'_>, Error> {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = unsafe { mdb_txn_begin(self.env, std::ptr::null_mut(), 0, &mut txn) };
        lmdb_result(rc)?;
        Ok(RwTxn {
            txn,
            _marker: PhantomData,
        })
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        unsafe { mdb_env_close(self.env) }
    }
}

// ---------------------------------------------------------------------------
// EnvBuilder
// ---------------------------------------------------------------------------

/// Builder for configuring and opening an LMDB [`Env`].
pub struct EnvBuilder {
    env: *mut MDB_env,
}

impl EnvBuilder {
    /// Create a new builder. Allocates an `MDB_env` handle internally.
    pub fn new() -> Self {
        let mut env: *mut MDB_env = std::ptr::null_mut();
        let rc = unsafe { mdb_env_create(&mut env) };
        assert_eq!(rc, 0, "mdb_env_create failed: {rc}");
        Self { env }
    }

    /// Set the maximum number of named databases.
    pub fn max_dbs(self, n: u32) -> Self {
        unsafe { mdb_env_set_maxdbs(self.env, n) };
        self
    }

    /// Set the memory map size in bytes.
    pub fn map_size(self, size: usize) -> Self {
        unsafe { mdb_env_set_mapsize(self.env, size) };
        self
    }

    /// Open the environment at the given path with the specified UNIX permissions.
    pub fn open(self, path: &Path, mode: u32) -> Result<Env, Error> {
        let path_str = path.to_str().expect("LMDB path must be valid UTF-8");
        let c_path = CString::new(path_str).expect("path must not contain null bytes");
        let rc = unsafe { mdb_env_open(self.env, c_path.as_ptr(), 0, mode as mdb_mode_t) };
        if rc != 0 {
            // Don't close in Drop — mdb_env_open failure leaves env in undefined state,
            // but mdb_env_close is still required to free the handle.
            // We'll let Drop handle it.
            return Err(Error(rc));
        }
        let env = Env { env: self.env };
        std::mem::forget(self); // prevent Drop from closing the env
        Ok(env)
    }
}

impl Drop for EnvBuilder {
    fn drop(&mut self) {
        // If open() was never called (or failed and we reach here),
        // we must free the allocated MDB_env handle.
        unsafe { mdb_env_close(self.env) }
    }
}

// ---------------------------------------------------------------------------
// Dbi (database handle)
// ---------------------------------------------------------------------------

/// Lightweight database handle (wraps `MDB_dbi`, which is just a `c_uint`).
#[derive(Clone, Copy)]
pub struct Dbi(MDB_dbi);

// ---------------------------------------------------------------------------
// RwTxn (read-write transaction)
// ---------------------------------------------------------------------------

/// Read-write LMDB transaction. Aborts on drop unless [`commit`](RwTxn::commit) is called.
pub struct RwTxn<'env> {
    txn: *mut MDB_txn,
    _marker: PhantomData<&'env Env>,
}

impl<'env> RwTxn<'env> {
    /// Create a [`Database`] view for the given database handle.
    pub fn bind(&self, dbi: &Dbi) -> Database<'_> {
        Database {
            txn: self.txn,
            dbi: dbi.0,
            _marker: PhantomData,
        }
    }

    /// Commit the transaction, persisting all changes.
    pub fn commit(self) -> Result<(), Error> {
        let rc = unsafe { mdb_txn_commit(self.txn) };
        std::mem::forget(self); // prevent Drop from aborting
        lmdb_result(rc)
    }

    pub fn as_raw(&self) -> *mut MDB_txn {
        self.txn
    }

    pub fn from_raw(txn: *mut MDB_txn) -> Self {
        Self {
            txn,
            _marker: PhantomData,
        }
    }
}

impl Drop for RwTxn<'_> {
    fn drop(&mut self) {
        unsafe { mdb_txn_abort(self.txn) }
    }
}

// ---------------------------------------------------------------------------
// Database (transaction + dbi view)
// ---------------------------------------------------------------------------

/// A view combining a transaction with a database handle.
///
/// This is a lightweight, non-owning type (`Copy`). The `'txn` lifetime
/// ensures it cannot outlive the transaction it was created from.
#[derive(Clone, Copy)]
pub struct Database<'txn> {
    txn: *mut MDB_txn,
    dbi: MDB_dbi,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> Database<'txn> {
    /// Retrieve the value for a key. Returns borrowed bytes valid for `'txn`.
    pub fn get(&self, key: &[u8]) -> Result<&'txn [u8], Error> {
        let mut key_val = to_mdb_val(key);
        let mut data_val = empty_mdb_val();
        let rc = unsafe { mdb_get(self.txn, self.dbi, &mut key_val, &mut data_val) };
        lmdb_result(rc)?;
        Ok(unsafe { from_mdb_val(&data_val) })
    }

    /// Store a key-value pair (overwrites any existing value for the key).
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut key_val = to_mdb_val(key);
        let mut data_val = to_mdb_val(value);
        let rc = unsafe { mdb_put(self.txn, self.dbi, &mut key_val, &mut data_val, 0) };
        lmdb_result(rc)
    }

    pub fn del(&self, key: &[u8]) -> Result<(), Error> {
        let mut key_val = to_mdb_val(key);
        let rc = unsafe { mdb_del(self.txn, self.dbi, &mut key_val, null_mut()) };
        lmdb_result(rc)
    }

    /// Open a new cursor on this database.
    pub fn cursor(&self) -> Result<Cursor<'txn>, Error> {
        let mut cursor: *mut MDB_cursor = std::ptr::null_mut();
        let rc = unsafe { mdb_cursor_open(self.txn, self.dbi, &mut cursor) };
        lmdb_result(rc)?;
        Ok(Cursor {
            cursor,
            _marker: PhantomData,
        })
    }
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

/// Cursor for traversing and mutating entries in an LMDB database.
///
/// The `'txn` lifetime ensures the cursor (and data it returns) cannot
/// outlive the transaction.
pub struct Cursor<'txn> {
    cursor: *mut MDB_cursor,
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> Cursor<'txn> {
    /// Position the cursor at the exact key (`MDB_SET`).
    pub fn set_key(&mut self, key: &[u8]) -> Result<(), Error> {
        let mut key_val = to_mdb_val(key);
        let mut data_val = empty_mdb_val();
        let rc = unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut data_val, MDB_SET) };
        lmdb_result(rc)
    }

    /// Position the cursor at the first key ≥ `key` (`MDB_SET_RANGE`).
    pub fn set_range(&mut self, key: &[u8]) -> Result<(), Error> {
        let mut key_val = to_mdb_val(key);
        let mut data_val = empty_mdb_val();
        let rc = unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut data_val, MDB_SET_RANGE) };
        lmdb_result(rc)
    }

    /// Advance the cursor to the next entry (`MDB_NEXT`).
    pub fn next(&mut self) -> Result<(), Error> {
        let mut key_val = empty_mdb_val();
        let mut data_val = empty_mdb_val();
        let rc = unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut data_val, MDB_NEXT) };
        lmdb_result(rc)
    }

    /// Move the cursor to the previous entry (`MDB_PREV`).
    pub fn prev(&mut self) -> Result<(), Error> {
        let mut key_val = empty_mdb_val();
        let mut data_val = empty_mdb_val();
        let rc = unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut data_val, MDB_PREV) };
        lmdb_result(rc)
    }

    /// Return the key at the current cursor position (`MDB_GET_CURRENT`).
    pub fn key(&self) -> Result<&'txn [u8], Error> {
        let mut key_val = empty_mdb_val();
        let mut data_val = empty_mdb_val();
        let rc =
            unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut data_val, MDB_GET_CURRENT) };
        lmdb_result(rc)?;
        Ok(unsafe { from_mdb_val(&key_val) })
    }

    /// Return the value at the current cursor position (`MDB_GET_CURRENT`).
    pub fn value(&self) -> Result<&'txn [u8], Error> {
        let mut key_val = empty_mdb_val();
        let mut data_val = empty_mdb_val();
        let rc =
            unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut data_val, MDB_GET_CURRENT) };
        lmdb_result(rc)?;
        Ok(unsafe { from_mdb_val(&data_val) })
    }

    /// Write a key-value pair via the cursor (`mdb_cursor_put`).
    pub fn put(&mut self, key: &[u8], value: &[u8], flags: u32) -> Result<(), Error> {
        let mut key_val = to_mdb_val(key);
        let mut data_val = to_mdb_val(value);
        let rc = unsafe { mdb_cursor_put(self.cursor, &mut key_val, &mut data_val, flags) };
        lmdb_result(rc)
    }

    /// Replace the value at the current cursor position (`MDB_CURRENT`).
    ///
    /// Reads the current key internally, then overwrites the value.
    pub fn put_current(&mut self, value: &[u8]) -> Result<(), Error> {
        // Read current key (required by MDB_CURRENT).
        let mut key_val = empty_mdb_val();
        let mut old_data = empty_mdb_val();
        let rc =
            unsafe { mdb_cursor_get(self.cursor, &mut key_val, &mut old_data, MDB_GET_CURRENT) };
        lmdb_result(rc)?;
        // Overwrite value in place.
        let mut new_data = to_mdb_val(value);
        let rc = unsafe { mdb_cursor_put(self.cursor, &mut key_val, &mut new_data, MDB_CURRENT) };
        lmdb_result(rc)
    }

    /// Delete the entry at the current cursor position.
    pub fn del(&mut self) -> Result<(), Error> {
        let rc = unsafe { mdb_cursor_del(self.cursor, 0) };
        lmdb_result(rc)
    }
}

impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        unsafe { mdb_cursor_close(self.cursor) }
    }
}

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// Flag for `mdb_dbi_open`: create the database if it doesn't exist.
pub const MDB_DB_CREATE: u32 = MDB_CREATE;
