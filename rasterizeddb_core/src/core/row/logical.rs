use std::fmt::Debug;

use crate::core::{
    db_type::DbType,
    processor::transformer::ComparerOperation,
    value_codec::{INET_LEN, UUID_LEN},
};

use super::{
    common::{FromLeBytes, FromLeBytesUnsafe, simd_compare_strings},
    error::EvalError,
};

#[inline(always)]
pub fn perform_comparison_operation(
    input1: &[u8],
    input2: &[u8],
    db_type: &DbType,
    operation: &ComparerOperation,
) -> std::result::Result<bool, EvalError> {
    // The structural operators — `@>`, `<@`, `&&`, the network family, `@@` —
    // ask a question about the value's shape rather than its order, so they are
    // answered by type in `pg_ops` rather than by the ordered comparisons
    // below. Handled first, before the dispatch, so no arm has to grow a case
    // that does not apply to it.
    //
    // `None` from `pg_ops` is an operator that has no meaning for these
    // operands, and becomes an error rather than `false`: answering `false`
    // would drop every row and look exactly like an empty table.
    if let ComparerOperation::Pg { op, right_type } = operation {
        return crate::core::pg_ops::evaluate(*op, db_type, input1, right_type, input2)
            .ok_or(EvalError::UnsupportedType);
    }

    // `SIMILAR TO` can refuse a value (too costly to match), and a refusal has
    // to fail the statement rather than read as "no match".
    if matches!(operation, ComparerOperation::SimilarTo)
        && matches!(db_type, DbType::STRING | DbType::JSONB)
    {
        return super::common::similar_to_compare(input1, input2);
    }

    match db_type {
        DbType::I8 => generic_comparison::<i8>(input1, input2, operation),
        DbType::U8 => generic_comparison::<u8>(input1, input2, operation),
        DbType::I16 => generic_comparison::<i16>(input1, input2, operation),
        DbType::U16 => generic_comparison::<u16>(input1, input2, operation),
        DbType::I32 => generic_comparison::<i32>(input1, input2, operation),
        DbType::U32 => generic_comparison::<u32>(input1, input2, operation),
        DbType::I64 => generic_comparison::<i64>(input1, input2, operation),
        DbType::U64 => generic_comparison::<u64>(input1, input2, operation),
        DbType::I128 => generic_comparison::<i128>(input1, input2, operation),
        DbType::U128 => generic_comparison::<u128>(input1, input2, operation),
        // PostgreSQL's float semantics, not IEEE's: NaN equals NaN and sorts
        // above every other value, and -0 equals +0. Under IEEE `==`,
        // `f = 'NaN'` matched nothing and `f > 1e308` never returned a NaN row,
        // while the sort path (`pg_float_cmp`) already put NaN last — two
        // answers to one question.
        DbType::F32 => float_comparison::<4>(input1, input2, operation),
        DbType::F64 => float_comparison::<8>(input1, input2, operation),
        DbType::STRING => Ok(string_comparison(input1, input2, operation)),
        // Stored as microseconds since the Unix epoch, so an instant comparison
        // is an `i64` comparison. Note this must NOT be a byte compare: the
        // value is little-endian, and pre-1970 instants are negative.
        DbType::DATETIME => generic_comparison::<i64>(input1, input2, operation),
        DbType::BOOL => generic_comparison::<u8>(input1, input2, operation),
        DbType::CHAR => generic_comparison::<u32>(input1, input2, operation),
        // Unscaled units at one fixed scale, so the integer order is the
        // numeric order and equal values are bitwise identical.
        DbType::DECIMAL => generic_comparison::<i128>(input1, input2, operation),
        // Fixed-width and big-endian by construction, so `memcmp` gives both
        // equality and the canonical ordering with no decoding step.
        DbType::UUID => bytes_comparison(input1, input2, UUID_LEN, operation),
        DbType::INET => bytes_comparison(input1, input2, INET_LEN, operation),
        // Equality is between values, not bytes: a number keeps its spelling
        // when it is encoded, so `1` and `1.0` differ as bytes and PostgreSQL
        // calls them equal, at any depth. A byte compare matched `doc = '1'`
        // against only one of the two. Bytes that are not an encoded document
        // fall back to the byte compare, which is all they ever had.
        //
        // The ordering operators follow PostgreSQL's `jsonb` order, the one
        // `ORDER BY doc` sorts by, rather than the bytes': `doc < '{"a":2}'`
        // asked whether one encoding precedes another, which put `{"a":10}`
        // and `{"b":0}` on the wrong side and told nothing about the values.
        DbType::JSONB => match operation {
            ComparerOperation::Equals | ComparerOperation::NotEquals => {
                let equal = crate::core::jsonb::values_equal(input1, input2)
                    .unwrap_or_else(|| input1 == input2);
                Ok(equal == matches!(operation, ComparerOperation::Equals))
            }
            ComparerOperation::Greater
            | ComparerOperation::GreaterOrEquals
            | ComparerOperation::Less
            | ComparerOperation::LessOrEquals => {
                ordered_comparison(input1, input2, db_type, operation)
            }
            _ => Ok(string_comparison(input1, input2, operation)),
        },
        // The types stored in PostgreSQL's own wire layout. Their ordering is
        // defined once, in `compare_bytes_as_type`, and reused here rather than
        // written a second time — the sort path and the filter path disagreeing
        // about which of two values is larger is not a failure that announces
        // itself.
        //
        // Without these arms a `DATE`, `TIME` or `INTERVAL` column could not be
        // compared at all: splitting them out of `DATETIME` gave them the right
        // wire type and took away every predicate over them.
        DbType::INTERVAL
        | DbType::MACADDR
        | DbType::MACADDR8
        | DbType::BIT
        | DbType::VARBIT
        | DbType::DATE
        | DbType::TIME
        | DbType::TIMETZ
        | DbType::POINT
        | DbType::LINE
        | DbType::LSEG
        | DbType::BOX
        | DbType::PATH
        | DbType::POLYGON
        | DbType::CIRCLE
        | DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => ordered_comparison(input1, input2, db_type, operation),
        // NULL / START / END / UNKNOWN are markers, not values. Returning an
        // error rather than panicking keeps a malformed query from taking a
        // worker down with it.
        _ => Err(EvalError::UnsupportedType),
    }
}

/// Comparison by the type's own ordering, for the types whose bytes are not
/// directly comparable.
///
/// The substring and case-folding operators have no meaning for any of them, so
/// they are refused rather than answered — a `false` there would silently drop
/// every row.
#[inline]
fn ordered_comparison(
    input1: &[u8],
    input2: &[u8],
    db_type: &DbType,
    operation: &ComparerOperation,
) -> std::result::Result<bool, EvalError> {
    use std::cmp::Ordering;
    let ordering = crate::core::processor::concurrent_processor::compare_bytes_as_type(
        db_type.clone(),
        input1,
        input2,
    );
    Ok(match operation {
        ComparerOperation::Equals => ordering == Ordering::Equal,
        ComparerOperation::NotEquals => ordering != Ordering::Equal,
        ComparerOperation::Greater => ordering == Ordering::Greater,
        ComparerOperation::GreaterOrEquals => ordering != Ordering::Less,
        ComparerOperation::Less => ordering == Ordering::Less,
        ComparerOperation::LessOrEquals => ordering != Ordering::Greater,
        _ => return Err(EvalError::UnsupportedType),
    })
}

/// Ordered comparison of two fixed-width values by their raw bytes.
///
/// Only correct for types stored big-endian, which is why it is not offered
/// generically — a little-endian value compared this way would order by its
/// least significant byte.
#[inline(always)]
fn bytes_comparison(
    input1: &[u8],
    input2: &[u8],
    width: usize,
    operation: &ComparerOperation,
) -> std::result::Result<bool, EvalError> {
    if input1.len() != width || input2.len() != width {
        return Err(EvalError::OperandSizeMismatch);
    }

    Ok(match operation {
        ComparerOperation::Equals => input1 == input2,
        ComparerOperation::NotEquals => input1 != input2,
        ComparerOperation::Greater => input1 > input2,
        ComparerOperation::GreaterOrEquals => input1 >= input2,
        ComparerOperation::Less => input1 < input2,
        ComparerOperation::LessOrEquals => input1 <= input2,
        // Substring and case-folding operations have no meaning for an identity
        // or an address.
        ComparerOperation::Contains
        | ComparerOperation::StartsWith
        | ComparerOperation::EndsWith
        | ComparerOperation::EqualsCaseInsensitive
        | ComparerOperation::ContainsCaseInsensitive
        | ComparerOperation::StartsWithCaseInsensitive
        | ComparerOperation::EndsWithCaseInsensitive
        | ComparerOperation::Like
        | ComparerOperation::LikeCaseInsensitive
        // A pattern match over an identity or an address is not a question
        // with an answer; the string path is where `SIMILAR TO` belongs.
        | ComparerOperation::SimilarTo => return Err(EvalError::UnsupportedType),
        // Taken by `perform_comparison_operation` before the type dispatch, so
        // this is unreachable in practice; it is spelled out rather than left
        // to a catch-all so a later operator cannot land here silently.
        ComparerOperation::Pg { .. } => return Err(EvalError::UnsupportedType),
    })
}

#[inline(always)]
fn generic_comparison<T>(
    input1: &[u8],
    input2: &[u8],
    operation: &ComparerOperation,
) -> std::result::Result<bool, EvalError>
where
    T: Copy + Default + PartialEq + PartialOrd + FromLeBytes + FromLeBytesUnsafe + Debug,
{
    // `from_le_bytes_unchecked` does a raw `read_unaligned` of `size_of::<T>()`
    // bytes. Guarding that with `debug_assert` meant release builds read past
    // the end of a short slice — undefined behaviour, not merely a wrong answer.
    let size = std::mem::size_of::<T>();
    if input1.len() != size || input2.len() != size {
        return Err(EvalError::OperandSizeMismatch);
    }

    // SAFETY: both slices were just verified to be exactly `size_of::<T>()` bytes.
    let num1 = unsafe { T::from_le_bytes_unchecked(input1) };
    let num2 = unsafe { T::from_le_bytes_unchecked(input2) };

    Ok(match operation {
        ComparerOperation::Equals => num1 == num2,
        ComparerOperation::NotEquals => num1 != num2,
        ComparerOperation::Greater => num1 > num2,
        ComparerOperation::GreaterOrEquals => num1 >= num2,
        ComparerOperation::Less => num1 < num2,
        ComparerOperation::LessOrEquals => num1 <= num2,
        ComparerOperation::Contains
        | ComparerOperation::StartsWith
        | ComparerOperation::EndsWith
        | ComparerOperation::EqualsCaseInsensitive
        | ComparerOperation::ContainsCaseInsensitive
        | ComparerOperation::StartsWithCaseInsensitive
        | ComparerOperation::EndsWithCaseInsensitive
        | ComparerOperation::Like
        | ComparerOperation::LikeCaseInsensitive => {
            // Substring, pattern and case-folding operations are string-shaped.
            // A query reaching this with a numeric column is malformed, not
            // fatal.
            return Err(EvalError::UnsupportedType);
        }
        // As for the other pattern operators above: string-shaped.
        ComparerOperation::SimilarTo => return Err(EvalError::UnsupportedType),
        // As above: routed out before the dispatch.
        ComparerOperation::Pg { .. } => return Err(EvalError::UnsupportedType),
    })
}

/// A float comparison in PostgreSQL's total order — see
/// [`crate::core::processor::concurrent_processor::pg_float_cmp`].
#[inline(always)]
fn float_comparison<const N: usize>(
    input1: &[u8],
    input2: &[u8],
    operation: &ComparerOperation,
) -> std::result::Result<bool, EvalError> {
    let (Ok(a), Ok(b)) = (<[u8; N]>::try_from(input1), <[u8; N]>::try_from(input2)) else {
        return Err(EvalError::OperandSizeMismatch);
    };
    let (x, y) = if N == 4 {
        (
            f32::from_le_bytes([a[0], a[1], a[2], a[3]]) as f64,
            f32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f64,
        )
    } else {
        (
            f64::from_le_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]),
            f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
        )
    };
    use std::cmp::Ordering;
    let ordering = crate::core::processor::concurrent_processor::pg_float_cmp(x, y);
    Ok(match operation {
        ComparerOperation::Equals => ordering == Ordering::Equal,
        ComparerOperation::NotEquals => ordering != Ordering::Equal,
        ComparerOperation::Greater => ordering == Ordering::Greater,
        ComparerOperation::GreaterOrEquals => ordering != Ordering::Less,
        ComparerOperation::Less => ordering == Ordering::Less,
        ComparerOperation::LessOrEquals => ordering != Ordering::Greater,
        // String-shaped, as for the integers.
        _ => return Err(EvalError::UnsupportedType),
    })
}

#[inline(always)]
fn string_comparison(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool {
    simd_compare_strings(input1, input2, operation)
}

#[cfg(test)]
mod tests {
    use crate::core::{
        db_type::DbType, processor::transformer::ComparerOperation,
        row::logical::perform_comparison_operation,
    };

    #[test]
    fn test_numeric_comparisons() {
        let input1 = 10i32.to_le_bytes();
        let input2 = 5i32.to_le_bytes();

        assert!(
            perform_comparison_operation(
                &input1,
                &input1,
                &DbType::I32,
                &ComparerOperation::Equals
            )
            .unwrap()
        );
        assert!(
            !perform_comparison_operation(
                &input1,
                &input2,
                &DbType::I32,
                &ComparerOperation::Equals
            )
            .unwrap()
        );
        assert!(
            perform_comparison_operation(&input2, &input1, &DbType::I32, &ComparerOperation::Less)
                .unwrap()
        );
        assert!(
            perform_comparison_operation(
                &input1,
                &input2,
                &DbType::I32,
                &ComparerOperation::Greater
            )
            .unwrap()
        );
        assert!(
            perform_comparison_operation(
                &input1,
                &input1,
                &DbType::I32,
                &ComparerOperation::GreaterOrEquals
            )
            .unwrap()
        );
        assert!(
            perform_comparison_operation(
                &input2,
                &input1,
                &DbType::I32,
                &ComparerOperation::LessOrEquals
            )
            .unwrap()
        );
    }

    /// PostgreSQL's float order over the special values: NaN = NaN and is
    /// above +infinity; -0 = +0. IEEE `==` answered "no" to NaN = NaN, so
    /// `WHERE f = 'NaN'` matched nothing.
    #[test]
    fn float_predicates_follow_postgres_not_ieee() {
        use ComparerOperation::*;
        let values = [
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            2.0,
            f64::INFINITY,
            f64::NAN,
        ];
        // Rank in PostgreSQL's order; -0 and +0 share one.
        let rank = |v: f64| -> i32 {
            if v.is_nan() {
                10
            } else if v == f64::NEG_INFINITY {
                0
            } else if v == -1.5 {
                1
            } else if v == 0.0 {
                2
            } else if v == 2.0 {
                3
            } else {
                4
            }
        };
        for &a in &values {
            for &b in &values {
                let (ra, rb) = (rank(a), rank(b));
                for (op, want) in [
                    (Equals, ra == rb),
                    (NotEquals, ra != rb),
                    (Greater, ra > rb),
                    (GreaterOrEquals, ra >= rb),
                    (Less, ra < rb),
                    (LessOrEquals, ra <= rb),
                ] {
                    let got64 = perform_comparison_operation(
                        &a.to_le_bytes(),
                        &b.to_le_bytes(),
                        &DbType::F64,
                        &op,
                    )
                    .unwrap();
                    assert_eq!(got64, want, "f64 {a} {op:?} {b}");
                    let got32 = perform_comparison_operation(
                        &(a as f32).to_le_bytes(),
                        &(b as f32).to_le_bytes(),
                        &DbType::F32,
                        &op,
                    )
                    .unwrap();
                    assert_eq!(got32, want, "f32 {a} {op:?} {b}");
                }
            }
        }
        // A NaN with another payload is still NaN.
        let other_nan = f64::from_bits(0xfff8_0000_0000_0001);
        assert!(
            perform_comparison_operation(
                &other_nan.to_le_bytes(),
                &f64::NAN.to_le_bytes(),
                &DbType::F64,
                &Equals
            )
            .unwrap()
        );
    }

    #[test]
    fn test_string_comparisons() {
        let input1 = "hello".as_bytes();
        let input2 = "world".as_bytes();
        let input3 = "hello".as_bytes();

        assert!(
            perform_comparison_operation(
                input1,
                input3,
                &DbType::STRING,
                &ComparerOperation::Equals
            )
            .unwrap()
        );
        assert!(
            !perform_comparison_operation(
                input1,
                input2,
                &DbType::STRING,
                &ComparerOperation::Equals
            )
            .unwrap()
        );
        assert!(
            perform_comparison_operation(
                input1,
                input2,
                &DbType::STRING,
                &ComparerOperation::NotEquals
            )
            .unwrap()
        );
    }

    /// Instants are microseconds since the Unix epoch, held little-endian. A
    /// byte compare would order them by their least significant byte, so this
    /// checks the ordering itself and not merely that the call returns.
    #[test]
    fn datetime_compares_as_a_signed_instant() {
        let earlier = 1_000_000i64.to_le_bytes();
        let later = 2_000_000i64.to_le_bytes();
        // 0x0100 vs 0x00FF: little-endian byte order disagrees with numeric order.
        let low_byte_trap = 256i64.to_le_bytes();
        let high_low_byte = 255i64.to_le_bytes();
        let before_epoch = (-1_000_000i64).to_le_bytes();

        let lt = |a: &[u8], b: &[u8]| {
            perform_comparison_operation(a, b, &DbType::DATETIME, &ComparerOperation::Less).unwrap()
        };

        assert!(lt(&earlier, &later));
        assert!(!lt(&later, &earlier));
        assert!(
            lt(&high_low_byte, &low_byte_trap),
            "must not compare byte-wise"
        );
        assert!(lt(&before_epoch, &earlier), "pre-1970 instants sort first");
        assert!(
            perform_comparison_operation(
                &earlier,
                &earlier,
                &DbType::DATETIME,
                &ComparerOperation::Equals
            )
            .unwrap()
        );
    }

    /// `WHERE name < 'abc'` is ordinary SQL. These three operators used to
    /// panic the worker evaluating them, so this checks the ordering itself
    /// rather than merely that the call returns.
    #[test]
    fn strings_order_lexicographically_instead_of_panicking() {
        let cmp = |a: &str, b: &str, op: ComparerOperation| {
            perform_comparison_operation(a.as_bytes(), b.as_bytes(), &DbType::STRING, &op).unwrap()
        };

        assert!(cmp("abc", "abd", ComparerOperation::Less));
        assert!(!cmp("abd", "abc", ComparerOperation::Less));
        assert!(cmp("abd", "abc", ComparerOperation::Greater));
        assert!(!cmp("abc", "abd", ComparerOperation::Greater));

        // Equal values are neither less nor greater, but satisfy both
        // "or equals" forms.
        assert!(!cmp("abc", "abc", ComparerOperation::Less));
        assert!(!cmp("abc", "abc", ComparerOperation::Greater));
        assert!(cmp("abc", "abc", ComparerOperation::LessOrEquals));
        assert!(cmp("abc", "abc", ComparerOperation::GreaterOrEquals));
    }

    /// A prefix sorts before the string that extends it, and length only
    /// decides once every shared byte is equal.
    #[test]
    fn a_prefix_sorts_before_its_extension() {
        let cmp = |a: &str, b: &str, op: ComparerOperation| {
            perform_comparison_operation(a.as_bytes(), b.as_bytes(), &DbType::STRING, &op).unwrap()
        };

        assert!(cmp("ab", "abc", ComparerOperation::Less));
        assert!(cmp("abc", "ab", ComparerOperation::Greater));
        assert!(cmp("", "a", ComparerOperation::Less));
        assert!(cmp("", "", ComparerOperation::LessOrEquals));
        // A longer string that differs early still loses on the first byte.
        assert!(cmp("b", "abcdef", ComparerOperation::Greater));
    }

    /// The four orderings must stay mutually consistent, since three of them
    /// are derived from the fourth.
    #[test]
    fn string_orderings_agree_with_each_other() {
        let cases = [
            "",
            "a",
            "ab",
            "abc",
            "abd",
            "b",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ];
        for a in cases {
            for b in cases {
                let c = |op: ComparerOperation| {
                    perform_comparison_operation(a.as_bytes(), b.as_bytes(), &DbType::STRING, &op)
                        .unwrap()
                };
                let (lt, gt, le, ge, eq) = (
                    c(ComparerOperation::Less),
                    c(ComparerOperation::Greater),
                    c(ComparerOperation::LessOrEquals),
                    c(ComparerOperation::GreaterOrEquals),
                    c(ComparerOperation::Equals),
                );

                assert_eq!(lt, a < b, "{:?} < {:?}", a, b);
                assert_eq!(gt, a > b, "{:?} > {:?}", a, b);
                assert_eq!(le, a <= b, "{:?} <= {:?}", a, b);
                assert_eq!(ge, a >= b, "{:?} >= {:?}", a, b);
                assert_eq!(eq, a == b, "{:?} == {:?}", a, b);
                assert!(!(lt && gt), "{:?} and {:?} cannot be both", a, b);
                assert_eq!(le, lt || eq);
                assert_eq!(ge, gt || eq);
            }
        }
    }

    #[test]
    fn bool_and_char_compare_instead_of_panicking() {
        let t = [1u8];
        let f = [0u8];
        assert!(
            perform_comparison_operation(&t, &t, &DbType::BOOL, &ComparerOperation::Equals)
                .unwrap()
        );
        assert!(
            perform_comparison_operation(&t, &f, &DbType::BOOL, &ComparerOperation::NotEquals)
                .unwrap()
        );

        let a = ('a' as u32).to_le_bytes();
        let b = ('b' as u32).to_le_bytes();
        assert!(
            perform_comparison_operation(&a, &b, &DbType::CHAR, &ComparerOperation::Less).unwrap()
        );
    }

    #[test]
    fn decimal_compares_as_scaled_units() {
        let one_five = crate::core::value_codec::parse_decimal("1.5")
            .unwrap()
            .to_le_bytes();
        let one_five_padded = crate::core::value_codec::parse_decimal("1.500")
            .unwrap()
            .to_le_bytes();
        let two = crate::core::value_codec::parse_decimal("2")
            .unwrap()
            .to_le_bytes();
        let negative = crate::core::value_codec::parse_decimal("-1")
            .unwrap()
            .to_le_bytes();

        assert!(
            perform_comparison_operation(
                &one_five,
                &one_five_padded,
                &DbType::DECIMAL,
                &ComparerOperation::Equals
            )
            .unwrap(),
            "trailing zeroes must not change the stored units"
        );
        assert!(
            perform_comparison_operation(
                &one_five,
                &two,
                &DbType::DECIMAL,
                &ComparerOperation::Less
            )
            .unwrap()
        );
        assert!(
            perform_comparison_operation(
                &negative,
                &one_five,
                &DbType::DECIMAL,
                &ComparerOperation::Less
            )
            .unwrap()
        );
    }

    #[test]
    fn uuid_compares_bytewise_in_canonical_order() {
        use crate::core::value_codec::parse_uuid;
        let low = parse_uuid("00000000-0000-0000-0000-000000000001").unwrap();
        let high = parse_uuid("ffffffff-0000-0000-0000-000000000000").unwrap();

        assert!(
            perform_comparison_operation(&low, &low, &DbType::UUID, &ComparerOperation::Equals)
                .unwrap()
        );
        assert!(
            perform_comparison_operation(&low, &high, &DbType::UUID, &ComparerOperation::Less)
                .unwrap()
        );
        assert!(
            perform_comparison_operation(&high, &low, &DbType::UUID, &ComparerOperation::Greater)
                .unwrap()
        );
    }

    #[test]
    fn inet_compares_by_family_then_address() {
        use crate::core::value_codec::parse_inet;
        let v4_low = parse_inet("10.0.0.1").unwrap();
        let v4_high = parse_inet("10.0.0.2").unwrap();
        let v6 = parse_inet("::1").unwrap();

        assert!(
            perform_comparison_operation(
                &v4_low,
                &v4_high,
                &DbType::INET,
                &ComparerOperation::Less
            )
            .unwrap()
        );
        assert!(
            perform_comparison_operation(&v4_high, &v6, &DbType::INET, &ComparerOperation::Less)
                .unwrap(),
            "IPv4 sorts before IPv6"
        );
        assert!(
            perform_comparison_operation(
                &v4_low,
                &v4_low,
                &DbType::INET,
                &ComparerOperation::Equals
            )
            .unwrap()
        );
    }

    /// A width that does not match the column type must fail cleanly rather
    /// than reading past the end of the slice.
    #[test]
    fn mismatched_operand_width_is_an_error() {
        use crate::core::row::error::EvalError;
        let short = [0u8; 4];
        let full = [0u8; 16];
        assert_eq!(
            perform_comparison_operation(&short, &full, &DbType::UUID, &ComparerOperation::Equals),
            Err(EvalError::OperandSizeMismatch)
        );
        assert_eq!(
            perform_comparison_operation(
                &short,
                &short,
                &DbType::DATETIME,
                &ComparerOperation::Equals
            ),
            Err(EvalError::OperandSizeMismatch)
        );
    }

    /// Marker types carry no value. They must report an error rather than take
    /// the worker down, since a query can reach them from user input.
    #[test]
    fn marker_types_report_an_error_instead_of_panicking() {
        use crate::core::row::error::EvalError;
        for ty in [DbType::NULL, DbType::START, DbType::END, DbType::UNKNOWN] {
            assert_eq!(
                perform_comparison_operation(&[0u8], &[0u8], &ty, &ComparerOperation::Equals),
                Err(EvalError::UnsupportedType),
                "{:?}",
                ty
            );
        }
    }

    #[test]
    fn substring_operations_are_rejected_for_identity_types() {
        use crate::core::row::error::EvalError;
        let a = [0u8; 16];
        assert_eq!(
            perform_comparison_operation(&a, &a, &DbType::UUID, &ComparerOperation::Contains),
            Err(EvalError::UnsupportedType)
        );
    }

    /// A substring or case-folding operator against a numeric column is a
    /// malformed query, not a fatal one. This used to panic, so `WHERE age
    /// CONTAINS 5` could take a query worker down.
    #[test]
    fn string_shaped_operators_on_numbers_are_an_error_not_a_panic() {
        use crate::core::row::error::EvalError;
        let a = 5i32.to_le_bytes();
        let b = 10i32.to_le_bytes();

        for op in [
            ComparerOperation::Contains,
            ComparerOperation::StartsWith,
            ComparerOperation::EndsWith,
            ComparerOperation::EqualsCaseInsensitive,
            ComparerOperation::ContainsCaseInsensitive,
            ComparerOperation::StartsWithCaseInsensitive,
            ComparerOperation::EndsWithCaseInsensitive,
        ] {
            assert_eq!(
                perform_comparison_operation(&a, &b, &DbType::I32, &op),
                Err(EvalError::UnsupportedType),
                "{:?}",
                op
            );
        }
    }

    #[test]
    fn case_insensitive_string_operators_ignore_ascii_case() {
        let cmp = |a: &str, b: &str, op: ComparerOperation| {
            perform_comparison_operation(a.as_bytes(), b.as_bytes(), &DbType::STRING, &op).unwrap()
        };

        assert!(cmp(
            "Acme",
            "ACME",
            ComparerOperation::EqualsCaseInsensitive
        ));
        assert!(!cmp(
            "Acme",
            "Acme Ltd",
            ComparerOperation::EqualsCaseInsensitive
        ));
        assert!(cmp(
            "alice@ACME.test",
            "acme",
            ComparerOperation::ContainsCaseInsensitive
        ));
        assert!(cmp(
            "Alice",
            "AL",
            ComparerOperation::StartsWithCaseInsensitive
        ));
        assert!(cmp(
            "Alice",
            "CE",
            ComparerOperation::EndsWithCaseInsensitive
        ));
        assert!(!cmp(
            "Alice",
            "zz",
            ComparerOperation::ContainsCaseInsensitive
        ));

        // The case-sensitive operators must be unaffected.
        assert!(!cmp("Acme", "ACME", ComparerOperation::Equals));
    }

    /// Folding is a letter's, never a neighbouring byte's: the boundary bytes
    /// around the ASCII alphabet must not be swept into the ASCII fast path's
    /// fold — `'_'` (0x5F) sits just below `'a'`-0x20, and `'{'` (0x7B) just
    /// above `'z'`-0x20. Non-ASCII letters fold as `lower()` folds them, which
    /// is PostgreSQL's definition of `ILIKE`; this test once pinned the
    /// ASCII-only behaviour (`'É'` not matching `'é'`), which was the bug.
    #[test]
    fn case_folding_does_not_reach_beyond_the_ascii_alphabet() {
        let cmp = |a: &[u8], b: &[u8]| {
            perform_comparison_operation(
                a,
                b,
                &DbType::STRING,
                &ComparerOperation::EqualsCaseInsensitive,
            )
            .unwrap()
        };

        assert!(!cmp(b"_", b"?"), "0x5F must not fold onto 0x3F");
        assert!(!cmp(b"{", b"["), "0x7B must not fold onto 0x5B");
        assert!(cmp(b"[", b"["));
        assert!(cmp("\u{00c9}".as_bytes(), "\u{00c9}".as_bytes()));
        // `'É' ILIKE 'é'`: true in PostgreSQL, as `lower('É') = 'é'` is.
        assert!(cmp("\u{00c9}".as_bytes(), "\u{00e9}".as_bytes()));
        assert!(cmp("\u{00e9}".as_bytes(), "\u{00c9}".as_bytes()));
        // A different letter still differs, accent or not.
        assert!(!cmp("\u{00c9}".as_bytes(), b"e"));
        assert!(!cmp("\u{00c9}".as_bytes(), "\u{00c8}".as_bytes()));
        // Bytes that are not text fold ASCII only and otherwise compare as
        // themselves: 0xC9 is not a letter in any encoding this reads.
        assert!(!cmp(&[0xC9], &[0xE9]));
        assert!(cmp(&[0xC9, b'A'], &[0xC9, b'a']));
    }

    /// Substring operations are meaningful for strings and meaningless for the
    /// fixed-width identity types, which is the split this checks.
    #[test]
    fn substring_operations_are_accepted_for_strings() {
        assert!(
            perform_comparison_operation(
                "hello".as_bytes(),
                "ell".as_bytes(),
                &DbType::STRING,
                &ComparerOperation::Contains
            )
            .unwrap()
        );
    }
}
