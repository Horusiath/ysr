use crate::lib0::{Value, F64_MAX_SAFE_INTEGER, F64_MIN_SAFE_INTEGER};
use bytes::Bytes;
use std::collections::HashMap;
use std::convert::TryFrom;

macro_rules! impl_from_num {
    ($t:ty) => {
        impl From<$t> for Value {
            #[inline]
            fn from(v: $t) -> Self {
                Self::Float(v as f64)
            }
        }

        impl TryFrom<Value> for $t {
            type Error = Value;

            fn try_from(v: Value) -> Result<Self, Self::Error> {
                match v {
                    Value::Float(num) => Ok(num as Self),
                    Value::Int(num) => Ok(num as Self),
                    other => Err(other),
                }
            }
        }
    };
}
macro_rules! impl_from_bigint {
    ($t:ty) => {
        impl From<$t> for Value {
            fn from(value: $t) -> Self {
                let value = value as i64;
                if value <= F64_MAX_SAFE_INTEGER && value >= F64_MIN_SAFE_INTEGER {
                    let v = value as f64;
                    Self::Float(v)
                } else {
                    Self::Int(value)
                }
            }
        }

        impl TryFrom<Value> for $t {
            type Error = Value;

            fn try_from(v: Value) -> Result<Self, Self::Error> {
                match v {
                    Value::Float(num) => Ok(num as Self),
                    Value::Int(num) => Ok(num as Self),
                    other => Err(other),
                }
            }
        }
    };
}

impl_from_num!(f32);
impl_from_num!(f64);
impl_from_num!(i16);
impl_from_num!(i32);
impl_from_num!(u16);
impl_from_num!(u32);
impl_from_bigint!(i64);
impl_from_bigint!(isize);

impl TryFrom<u64> for Value {
    type Error = u64;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value > i64::MAX as u64 {
            Err(value)
        } else {
            let value = value as i64;
            if value <= F64_MAX_SAFE_INTEGER && value >= F64_MIN_SAFE_INTEGER {
                let v = value as f64;
                Ok(Value::Float(v))
            } else {
                Ok(Value::Int(value))
            }
        }
    }
}

impl TryFrom<Value> for u64 {
    type Error = Value;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Float(num) => Ok(num as Self),
            Value::Int(num) => Ok(num as Self),
            other => Err(other),
        }
    }
}

impl TryFrom<usize> for Value {
    type Error = usize;

    #[cfg(target_pointer_width = "32")]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        // for 32-bit architectures we know that usize will always fit,
        // so there's no need to check for length, but we stick to TryInto
        // trait to keep API compatibility
        Ok(Value::Float(value as f64))
    }

    #[cfg(target_pointer_width = "64")]
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        use std::convert::TryInto;
        if let Ok(v) = (value as u64).try_into() {
            Ok(v)
        } else {
            Err(value)
        }
    }
}

impl TryFrom<Value> for usize {
    type Error = Value;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Float(num) => Ok(num as Self),
            Value::Int(num) => Ok(num as Self),
            other => Err(other),
        }
    }
}

impl From<bool> for Value {
    #[inline]
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl TryFrom<Value> for bool {
    type Error = Value;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::Bool(num) => Ok(num),
            other => Err(other),
        }
    }
}

impl From<String> for Value {
    #[inline]
    fn from(value: String) -> Self {
        Value::String(value.into())
    }
}

impl TryFrom<Value> for String {
    type Error = Value;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::String(value) => Ok(value),
            other => Err(other),
        }
    }
}

impl From<&str> for Value {
    #[inline]
    fn from(value: &str) -> Self {
        Value::String(value.into())
    }
}

impl From<Vec<u8>> for Value {
    #[inline]
    fn from(value: Vec<u8>) -> Self {
        Value::ByteArray(value.into())
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Value;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::ByteArray(value) => Ok(Vec::from(value.as_ref())),
            other => Err(other),
        }
    }
}

impl From<Bytes> for Value {
    #[inline]
    fn from(value: Bytes) -> Self {
        Value::ByteArray(value)
    }
}

impl TryFrom<Value> for Bytes {
    type Error = Value;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v {
            Value::ByteArray(value) => Ok(value),
            other => Err(other),
        }
    }
}

impl From<&[u8]> for Value {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Value::ByteArray(Bytes::copy_from_slice(value))
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Option<T>) -> Value {
        match v {
            None => Value::Null,
            Some(value) => value.into(),
        }
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(v: Vec<T>) -> Value {
        let mut array = Vec::with_capacity(v.len());
        for value in v {
            array.push(value.into())
        }
        Value::Array(array)
    }
}

impl<T> From<HashMap<String, T>> for Value
where
    T: Into<Value>,
{
    fn from(v: HashMap<String, T>) -> Value {
        let mut map = HashMap::with_capacity(v.len());
        for (key, value) in v {
            map.insert(key, value.into());
        }
        Value::Object(map)
    }
}

// This code is based on serde_json::json! macro (see: https://docs.rs/serde_json/latest/src/serde_json/macros.rs.html#53-58).
// Kudos to the original authors.

/// Construct a lib0 [Value] value literal.
///
/// # Examples
///
/// ```rust
/// use ysr::lib0;
///
/// let value = lib0!({
///   "code": 200,
///   "success": true,
///   "payload": {
///     "features": [
///       "lib0",
///       true
///     ]
///   }
/// });
/// ```
#[macro_export(local_inner_macros)]
macro_rules! lib0 {
    // Hide distracting implementation details from the generated rustdoc.
    ($($any:tt)+) => {
        lib0_internal!($($any)+)
    };
}

#[macro_export(local_inner_macros)]
#[doc(hidden)]
macro_rules! lib0_internal {
    (@array [$($items:expr,)*]) => {
        lib0_internal_array![$($items,)*]
    };

    // Done without trailing comma.
    (@array [$($items:expr),*]) => {
        lib0_internal_array![$($items),*]
    };

    // Next item is `null`.
    (@array [$($items:expr,)*] null $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)* lib0_internal!(null)] $($rest)*)
    };

    // Next item is `true`.
    (@array [$($items:expr,)*] true $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)* lib0_internal!(true)] $($rest)*)
    };

    // Next item is `false`.
    (@array [$($items:expr,)*] false $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)* lib0_internal!(false)] $($rest)*)
    };

    // Next item is an array.
    (@array [$($items:expr,)*] [$($array:tt)*] $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)* lib0_internal!([$($array)*])] $($rest)*)
    };

    // Next item is a map.
    (@array [$($items:expr,)*] {$($map:tt)*} $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)* lib0_internal!({$($map)*})] $($rest)*)
    };

    // Next item is an expression followed by comma.
    (@array [$($items:expr,)*] $next:expr, $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)* lib0_internal!($next),] $($rest)*)
    };

    // Last item is an expression with no trailing comma.
    (@array [$($items:expr,)*] $last:expr) => {
        lib0_internal!(@array [$($items,)* lib0_internal!($last)])
    };

    // Comma after the most recent item.
    (@array [$($items:expr),*] , $($rest:tt)*) => {
        lib0_internal!(@array [$($items,)*] $($rest)*)
    };

    // Unexpected token after most recent item.
    (@array [$($items:expr),*] $unexpected:tt $($rest:tt)*) => {
        lib0_unexpected!($unexpected)
    };

    (@object $object:ident () () ()) => {};

    // Insert the current entry followed by trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr) , $($rest:tt)*) => {
        let _ = $object.insert(($($key)+).into(), $value);
        lib0_internal!(@object $object () ($($rest)*) ($($rest)*));
    };

    // Current entry followed by unexpected token.
    (@object $object:ident [$($key:tt)+] ($value:expr) $unexpected:tt $($rest:tt)*) => {
        lib0_unexpected!($unexpected);
    };

    // Insert the last entry without trailing comma.
    (@object $object:ident [$($key:tt)+] ($value:expr)) => {
        let _ = $object.insert(($($key)+).into(), $value);
    };

    // Next value is `null`.
    (@object $object:ident ($($key:tt)+) (: null $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!(null)) $($rest)*);
    };

    // Next value is `true`.
    (@object $object:ident ($($key:tt)+) (: true $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!(true)) $($rest)*);
    };

    // Next value is `false`.
    (@object $object:ident ($($key:tt)+) (: false $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!(false)) $($rest)*);
    };

    // Next value is an array.
    (@object $object:ident ($($key:tt)+) (: [$($array:tt)*] $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!([$($array)*])) $($rest)*);
    };

    // Next value is a map.
    (@object $object:ident ($($key:tt)+) (: {$($map:tt)*} $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!({$($map)*})) $($rest)*);
    };

    // Next value is an expression followed by comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr , $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!($value)) , $($rest)*);
    };

    // Last value is an expression with no trailing comma.
    (@object $object:ident ($($key:tt)+) (: $value:expr) $copy:tt) => {
        lib0_internal!(@object $object [$($key)+] (lib0_internal!($value)));
    };

    // Missing value for last entry. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)+) (:) $copy:tt) => {
        // "unexpected end of macro invocation"
        lib0_internal!();
    };

    // Missing colon and value for last entry. Trigger a reasonable error
    // message.
    (@object $object:ident ($($key:tt)+) () $copy:tt) => {
        // "unexpected end of macro invocation"
        lib0_internal!();
    };

    // Misplaced colon. Trigger a reasonable error message.
    (@object $object:ident () (: $($rest:tt)*) ($colon:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `:`".
        lib0_unexpected!($colon);
    };

    // Found a comma inside a key. Trigger a reasonable error message.
    (@object $object:ident ($($key:tt)*) (, $($rest:tt)*) ($comma:tt $($copy:tt)*)) => {
        // Takes no arguments so "no rules expected the token `,`".
        lib0_unexpected!($comma);
    };

    // Key is fully parenthesized. This avoids clippy double_parens false
    // positives because the parenthesization may be necessary here.
    (@object $object:ident () (($key:expr) : $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object ($key) (: $($rest)*) (: $($rest)*));
    };

    // Refuse to absorb colon token into key expression.
    (@object $object:ident ($($key:tt)*) (: $($unexpected:tt)+) $copy:tt) => {
        json_expect_expr_comma!($($unexpected)+);
    };

    // Munch a token into the current key.
    (@object $object:ident ($($key:tt)*) ($tt:tt $($rest:tt)*) $copy:tt) => {
        lib0_internal!(@object $object ($($key)* $tt) ($($rest)*) ($($rest)*));
    };

    //////////////////////////////////////////////////////////////////////////
    // The main implementation.
    //
    // Must be invoked as: lib0_internal!($($json)+)
    //////////////////////////////////////////////////////////////////////////

    (null) => {
        $crate::lib0::Value::Null
    };

    (true) => {
        $crate::lib0::Value::Bool(true)
    };

    (false) => {
        $crate::lib0::Value::Bool(false)
    };

    ([]) => {
        $crate::lib0::Value::Array(lib0_internal_array![])
    };

    ([ $($tt:tt)+ ]) => {
        $crate::lib0::Value::Array(lib0_internal!(@array [] $($tt)+))
    };

    ({}) => {
        $crate::lib0::Value::Object(std::collections::HashMap::new())
    };

    ({ $($tt:tt)+ }) => {
        $crate::lib0::Value::Object({
            let mut object = std::collections::HashMap::new();
            lib0_internal!(@object object () ($($tt)+) ($($tt)+));
            object
        })
    };

    // Value Serialize type: numbers, strings, struct literals, variables etc.
    // Must be below every other rule.
    ($other:expr) => {
        ($other).into()
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! lib0_internal_array {
    ($($content:tt)*) => {
        vec![$($content)*]
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! lib0_unexpected {
    () => {};
}

#[macro_export]
#[doc(hidden)]
macro_rules! lib0_expect_expr_comma {
    ($e:expr , $($tt:tt)*) => {};
}
