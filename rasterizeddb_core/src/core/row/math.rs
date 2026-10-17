use crate::{
    core::{db_type::DbType, processor::transformer::MathOperation},
    memory_pool::{MEMORY_POOL, MemoryBlock},
};

use super::common::{FromF64, FromLeBytes, IntoF64};
use super::error::EvalError;

/// Add/subtract/multiply that reports leaving the type's range instead of
/// wrapping.
///
/// The release profile sets `overflow-checks = false`, so plain `+`/`-`/`*` on
/// integers wrapped silently there — `WHERE salary * 1000000 > x` quietly
/// produced a wrong answer instead of an error. Integers are checked here;
/// floats keep IEEE semantics (saturating to infinity), which is what callers
/// expect from floating point.
pub trait CheckedArith: Sized {
    fn arith_add(self, rhs: Self) -> Option<Self>;
    fn arith_sub(self, rhs: Self) -> Option<Self>;
    fn arith_mul(self, rhs: Self) -> Option<Self>;
    /// The caller has refused a zero divisor; what is left to refuse is the
    /// one quotient that does not fit, `MIN / -1`.
    fn arith_div(self, rhs: Self) -> Option<Self>;
}

macro_rules! impl_checked_arith_int {
    ($($type:ty),*) => {
        $(
            impl CheckedArith for $type {
                #[inline(always)]
                fn arith_add(self, rhs: Self) -> Option<Self> { self.checked_add(rhs) }
                #[inline(always)]
                fn arith_sub(self, rhs: Self) -> Option<Self> { self.checked_sub(rhs) }
                #[inline(always)]
                fn arith_mul(self, rhs: Self) -> Option<Self> { self.checked_mul(rhs) }
                #[inline(always)]
                fn arith_div(self, rhs: Self) -> Option<Self> { self.checked_div(rhs) }
            }
        )*
    };
}

macro_rules! impl_checked_arith_float {
    ($($type:ty),*) => {
        $(
            impl CheckedArith for $type {
                #[inline(always)]
                fn arith_add(self, rhs: Self) -> Option<Self> { Some(self + rhs) }
                #[inline(always)]
                fn arith_sub(self, rhs: Self) -> Option<Self> { Some(self - rhs) }
                #[inline(always)]
                fn arith_mul(self, rhs: Self) -> Option<Self> { Some(self * rhs) }
                #[inline(always)]
                fn arith_div(self, rhs: Self) -> Option<Self> { Some(self / rhs) }
            }
        )*
    };
}

impl_checked_arith_int!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128);
impl_checked_arith_float!(f32, f64);

/// Function to perform a mathematical operation on two byte slices
/// each representing a numeric type. The numeric type is determined by the `db_type` parameter.
/// The operation is specified by the `operation` parameter.
/// The function returns a `MemoryBlock` containing the result of the operation.
/// The function uses a generic approach to handle different numeric types.
/// `&[u8]` must be in little-endian format.
#[inline(always)]
/// An eight-byte little-endian instant, or `None` when the operand is not one.
fn le_i64(bytes: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?))
}

/// Copies bytes into a pooled block.
fn block_of(bytes: &[u8]) -> MemoryBlock {
    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    mb
}

pub fn perform_math_operation(
    input1: &[u8],
    input2: &[u8],
    db_type: &DbType,
    operation: &MathOperation,
) -> std::result::Result<MemoryBlock, EvalError> {
    // The temporal operations pair operands of *different* widths on purpose —
    // an eight-byte instant against a sixteen-byte span — so they are answered
    // before the equal-width check below.
    match operation {
        MathOperation::IntervalBetween => {
            let (Some(a), Some(b)) = (le_i64(input1), le_i64(input2)) else {
                return Err(EvalError::OperandSizeMismatch);
            };
            let span = crate::core::pg_types::Interval {
                months: 0,
                days: 0,
                micros: a.checked_sub(b).ok_or(EvalError::UnsupportedType)?,
            };
            return Ok(block_of(&crate::core::pg_types::encode_interval(span)));
        }
        MathOperation::ShiftForward | MathOperation::ShiftBackward => {
            let Some(instant) = le_i64(input1) else {
                return Err(EvalError::OperandSizeMismatch);
            };
            let Some(mut span) = crate::core::pg_types::decode_interval(input2) else {
                return Err(EvalError::OperandSizeMismatch);
            };
            if matches!(operation, MathOperation::ShiftBackward) {
                span = crate::core::pg_types::Interval {
                    months: -span.months,
                    days: -span.days,
                    micros: -span.micros,
                };
            }
            let shifted = crate::core::pg_types::shift_timestamp(instant, span)
                .ok_or(EvalError::UnsupportedType)?;
            return Ok(block_of(&shifted.to_le_bytes()));
        }
        _ => {}
    }

    // Checked, not `debug_assert`ed: release builds compile assertions out, and
    // the mismatch then reaches `from_le_bytes`, which panics on a short slice.
    if input1.len() != input2.len() {
        return Err(EvalError::OperandSizeMismatch);
    }

    match db_type {
        DbType::I8 => generic_operation::<i8>(input1, input2, operation),
        DbType::U8 => generic_operation::<u8>(input1, input2, operation),
        DbType::I16 => generic_operation::<i16>(input1, input2, operation),
        DbType::U16 => generic_operation::<u16>(input1, input2, operation),
        DbType::I32 => generic_operation::<i32>(input1, input2, operation),
        DbType::U32 => generic_operation::<u32>(input1, input2, operation),
        DbType::I64 => generic_operation::<i64>(input1, input2, operation),
        DbType::U64 => generic_operation::<u64>(input1, input2, operation),
        DbType::I128 => generic_operation::<i128>(input1, input2, operation),
        DbType::U128 => generic_operation::<u128>(input1, input2, operation),
        DbType::F32 => generic_operation::<f32>(input1, input2, operation),
        DbType::F64 => generic_operation::<f64>(input1, input2, operation),
        DbType::DECIMAL => decimal_operation(input1, input2, operation),
        // Reported, not panicked: the type comes from a client's query, and a
        // panic here takes down the worker thread evaluating the row.
        _ => Err(EvalError::UnsupportedType),
    }
}

/// Arithmetic over two `NUMERIC` values, both at the engine scale.
///
/// The planner lifts a column read at its declared scale to the engine scale
/// before it gets here (see `ColumnTransformer::column_1_scale`) and encodes a
/// literal at the engine scale, so both operands share one. The product and
/// quotient are the expression evaluator's own (`expr::decimal_mul_engine`),
/// so `WHERE v * 2 = 200` and `SELECT v * 2` agree to the last digit.
///
/// Integer arithmetic on the raw `i128`s was the wrong answer this replaces in
/// principle — `v * 2` of two scaled values is scaled twice — though in
/// practice every such predicate was refused before reaching it, the literal
/// arriving at a different width.
fn decimal_operation(
    input1: &[u8],
    input2: &[u8],
    operation: &MathOperation,
) -> std::result::Result<MemoryBlock, EvalError> {
    let (Ok(a), Ok(b)) = (<[u8; 16]>::try_from(input1), <[u8; 16]>::try_from(input2)) else {
        return Err(EvalError::OperandSizeMismatch);
    };
    let (x, y) = (i128::from_le_bytes(a), i128::from_le_bytes(b));
    let value = match operation {
        MathOperation::Add => x.checked_add(y),
        MathOperation::Subtract => x.checked_sub(y),
        MathOperation::Multiply => crate::core::rql::expr::decimal_mul_engine(x, y),
        MathOperation::Divide => {
            if y == 0 {
                return Err(EvalError::DivisionByZero);
            }
            crate::core::rql::expr::decimal_div_engine(x, y)
        }
        // `^` and `|/` over NUMERIC would go through a float here and lose
        // digits silently; refused instead.
        _ => return Err(EvalError::UnsupportedType),
    }
    .ok_or(EvalError::ArithmeticOverflow)?;
    Ok(block_of(&value.to_le_bytes()))
}

#[inline(always)]
fn generic_operation<T>(
    input1: &[u8],
    input2: &[u8],
    operation: &MathOperation,
) -> std::result::Result<MemoryBlock, EvalError>
where
    T: Copy
        + Default
        + std::ops::Div<Output = T>
        + CheckedArith
        + PartialEq
        + FromLeBytes
        + FromF64
        + IntoF64,
{
    let size = std::mem::size_of::<T>();

    // `from_le_bytes` panics on a slice that isn't exactly `size` bytes, so this
    // has to be a real check rather than a debug assertion.
    if input1.len() != size || input2.len() != size {
        return Err(EvalError::OperandSizeMismatch);
    }

    let num1 = T::from_le_bytes(input1);
    let num2 = T::from_le_bytes(input2);

    let res = match operation {
        MathOperation::Add => num1.arith_add(num2).ok_or(EvalError::ArithmeticOverflow)?,
        MathOperation::Subtract => num1.arith_sub(num2).ok_or(EvalError::ArithmeticOverflow)?,
        MathOperation::Multiply => num1.arith_mul(num2).ok_or(EvalError::ArithmeticOverflow)?,
        MathOperation::Divide => {
            // Integer division by zero panics in every build profile — a
            // `debug_assert` here left release builds crashing the worker
            // thread on any row where the divisor happened to be 0.
            if num2 == T::default() {
                return Err(EvalError::DivisionByZero);
            }
            // `-2147483648 / -1` panics in every build profile, and took the
            // scan worker down with it; PostgreSQL answers it with
            // `integer out of range`.
            num1.arith_div(num2).ok_or(EvalError::ArithmeticOverflow)?
        }
        // Answered before the type dispatch, so reaching them here means the
        // planner paired a temporal operation with a numeric column.
        MathOperation::IntervalBetween
        | MathOperation::ShiftForward
        | MathOperation::ShiftBackward => return Err(EvalError::UnsupportedType),
        MathOperation::Exponent => {
            let base: f64 = num1.into_f64();
            let exp: f64 = num2.into_f64();
            T::from_f64(base.powf(exp))
        }
        MathOperation::Root => {
            let value: f64 = num1.into_f64();
            let root: f64 = num2.into_f64();
            if root == 0.0 {
                return Err(EvalError::RootByZero);
            }
            T::from_f64(value.powf(1.0 / root))
        }
    };

    let mut block = MEMORY_POOL.acquire(input1.len());
    let result = block.into_slice_mut();
    result.copy_from_slice(res.to_le_bytes().into_slice());

    Ok(block)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to convert a value to little-endian bytes
    fn to_le_bytes<T>(value: T) -> MemoryBlock
    where
        T: Copy + Default + FromLeBytes,
    {
        FromLeBytes::to_le_bytes(&value)
    }

    // Helper function to extract value from memory block
    fn from_memory_block<T>(block: MemoryBlock) -> T
    where
        T: Copy + Default + FromLeBytes,
    {
        let bytes = block.into_slice();
        T::from_le_bytes(bytes)
    }

    #[test]
    fn test_add_operation() {
        // Integer addition
        let result = perform_math_operation(
            to_le_bytes(5i32).into_slice(),
            to_le_bytes(3i32).into_slice(),
            &DbType::I32,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<i32>(result.unwrap()), 8);

        // Unsigned integer addition
        let result = perform_math_operation(
            to_le_bytes(5u64).into_slice(),
            to_le_bytes(3u64).into_slice(),
            &DbType::U64,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<u64>(result.unwrap()), 8);

        // Floating point addition
        let result = perform_math_operation(
            to_le_bytes(5.5f32).into_slice(),
            to_le_bytes(3.2f32).into_slice(),
            &DbType::F32,
            &MathOperation::Add,
        );
        assert!((from_memory_block::<f32>(result.unwrap()) - 8.7).abs() < 0.001);

        let result = perform_math_operation(
            to_le_bytes(5.5f64).into_slice(),
            to_le_bytes(3.2f64).into_slice(),
            &DbType::F64,
            &MathOperation::Add,
        );
        assert!((from_memory_block::<f64>(result.unwrap()) - 8.7).abs() < 0.001);
    }

    #[test]
    fn test_subtract_operation() {
        // Integer subtraction
        let result = perform_math_operation(
            to_le_bytes(5i32).into_slice(),
            to_le_bytes(3i32).into_slice(),
            &DbType::I32,
            &MathOperation::Subtract,
        );
        assert_eq!(from_memory_block::<i32>(result.unwrap()), 2);

        // Unsigned integer subtraction
        let result = perform_math_operation(
            to_le_bytes(5u64).into_slice(),
            to_le_bytes(3u64).into_slice(),
            &DbType::U64,
            &MathOperation::Subtract,
        );
        assert_eq!(from_memory_block::<u64>(result.unwrap()), 2);

        // Floating point subtraction
        let result = perform_math_operation(
            to_le_bytes(5.5f32).into_slice(),
            to_le_bytes(3.2f32).into_slice(),
            &DbType::F32,
            &MathOperation::Subtract,
        );
        assert!((from_memory_block::<f32>(result.unwrap()) - 2.3).abs() < 0.001);

        let result = perform_math_operation(
            to_le_bytes(5.5f64).into_slice(),
            to_le_bytes(3.2f64).into_slice(),
            &DbType::F64,
            &MathOperation::Subtract,
        );
        assert!((from_memory_block::<f64>(result.unwrap()) - 2.3).abs() < 0.001);
    }

    #[test]
    fn test_multiply_operation() {
        // Integer multiplication
        let result = perform_math_operation(
            to_le_bytes(5i32).into_slice(),
            to_le_bytes(3i32).into_slice(),
            &DbType::I32,
            &MathOperation::Multiply,
        );
        assert_eq!(from_memory_block::<i32>(result.unwrap()), 15);

        // Unsigned integer multiplication
        let result = perform_math_operation(
            to_le_bytes(5u64).into_slice(),
            to_le_bytes(3u64).into_slice(),
            &DbType::U64,
            &MathOperation::Multiply,
        );
        assert_eq!(from_memory_block::<u64>(result.unwrap()), 15);

        // Floating point multiplication
        let result = perform_math_operation(
            to_le_bytes(5.5f32).into_slice(),
            to_le_bytes(3.0f32).into_slice(),
            &DbType::F32,
            &MathOperation::Multiply,
        );
        assert!((from_memory_block::<f32>(result.unwrap()) - 16.5).abs() < 0.001);

        let result = perform_math_operation(
            to_le_bytes(5.5f64).into_slice(),
            to_le_bytes(3.0f64).into_slice(),
            &DbType::F64,
            &MathOperation::Multiply,
        );
        assert!((from_memory_block::<f64>(result.unwrap()) - 16.5).abs() < 0.001);
    }

    #[test]
    fn test_divide_operation() {
        // Integer division
        let result = perform_math_operation(
            to_le_bytes(15i32).into_slice(),
            to_le_bytes(3i32).into_slice(),
            &DbType::I32,
            &MathOperation::Divide,
        );
        assert_eq!(from_memory_block::<i32>(result.unwrap()), 5);

        // Unsigned integer division
        let result = perform_math_operation(
            to_le_bytes(15u64).into_slice(),
            to_le_bytes(3u64).into_slice(),
            &DbType::U64,
            &MathOperation::Divide,
        );
        assert_eq!(from_memory_block::<u64>(result.unwrap()), 5);

        // Floating point division
        let result = perform_math_operation(
            to_le_bytes(16.5f32).into_slice(),
            to_le_bytes(3.0f32).into_slice(),
            &DbType::F32,
            &MathOperation::Divide,
        );
        assert!((from_memory_block::<f32>(result.unwrap()) - 5.5).abs() < 0.001);

        let result = perform_math_operation(
            to_le_bytes(16.5f64).into_slice(),
            to_le_bytes(3.0f64).into_slice(),
            &DbType::F64,
            &MathOperation::Divide,
        );
        assert!((from_memory_block::<f64>(result.unwrap()) - 5.5).abs() < 0.001);
    }

    #[test]
    fn test_exponent_operation() {
        // Integer exponentiation
        let result = perform_math_operation(
            to_le_bytes(2i32).into_slice(),
            to_le_bytes(3i32).into_slice(),
            &DbType::I32,
            &MathOperation::Exponent,
        );
        assert_eq!(from_memory_block::<i32>(result.unwrap()), 8);

        // Unsigned integer exponentiation
        let result = perform_math_operation(
            to_le_bytes(2u64).into_slice(),
            to_le_bytes(3u64).into_slice(),
            &DbType::U64,
            &MathOperation::Exponent,
        );
        assert_eq!(from_memory_block::<u64>(result.unwrap()), 8);

        // Floating point exponentiation
        let result = perform_math_operation(
            to_le_bytes(2.0f32).into_slice(),
            to_le_bytes(1.5f32).into_slice(),
            &DbType::F32,
            &MathOperation::Exponent,
        );
        assert!((from_memory_block::<f32>(result.unwrap()) - 2.83).abs() < 0.01);

        let result = perform_math_operation(
            to_le_bytes(2.0f64).into_slice(),
            to_le_bytes(1.5f64).into_slice(),
            &DbType::F64,
            &MathOperation::Exponent,
        );
        assert!((from_memory_block::<f64>(result.unwrap()) - 2.83).abs() < 0.01);
    }

    #[test]
    fn test_root_operation() {
        // Integer root
        let result = perform_math_operation(
            to_le_bytes(8i32).into_slice(),
            to_le_bytes(3i32).into_slice(),
            &DbType::I32,
            &MathOperation::Root,
        );
        assert_eq!(from_memory_block::<i32>(result.unwrap()), 2);

        // Unsigned integer root
        let result = perform_math_operation(
            to_le_bytes(8u64).into_slice(),
            to_le_bytes(3u64).into_slice(),
            &DbType::U64,
            &MathOperation::Root,
        );
        assert_eq!(from_memory_block::<u64>(result.unwrap()), 2);

        // Floating point root
        let result = perform_math_operation(
            to_le_bytes(9.0f32).into_slice(),
            to_le_bytes(2.0f32).into_slice(),
            &DbType::F32,
            &MathOperation::Root,
        );
        assert!((from_memory_block::<f32>(result.unwrap()) - 3.0).abs() < 0.001);

        let result = perform_math_operation(
            to_le_bytes(9.0f64).into_slice(),
            to_le_bytes(2.0f64).into_slice(),
            &DbType::F64,
            &MathOperation::Root,
        );
        assert!((from_memory_block::<f64>(result.unwrap()) - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_all_numeric_types() {
        // Test i8
        let result = perform_math_operation(
            to_le_bytes(5i8).into_slice(),
            to_le_bytes(3i8).into_slice(),
            &DbType::I8,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<i8>(result.unwrap()), 8);

        // Test u8
        let result = perform_math_operation(
            to_le_bytes(5u8).into_slice(),
            to_le_bytes(3u8).into_slice(),
            &DbType::U8,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<u8>(result.unwrap()), 8);

        // Test i16
        let result = perform_math_operation(
            to_le_bytes(5i16).into_slice(),
            to_le_bytes(3i16).into_slice(),
            &DbType::I16,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<i16>(result.unwrap()), 8);

        // Test u16
        let result = perform_math_operation(
            to_le_bytes(5u16).into_slice(),
            to_le_bytes(3u16).into_slice(),
            &DbType::U16,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<u16>(result.unwrap()), 8);

        // Test i128
        let result = perform_math_operation(
            to_le_bytes(5i128).into_slice(),
            to_le_bytes(3i128).into_slice(),
            &DbType::I128,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<i128>(result.unwrap()), 8);

        // Test u128
        let result = perform_math_operation(
            to_le_bytes(5u128).into_slice(),
            to_le_bytes(3u128).into_slice(),
            &DbType::U128,
            &MathOperation::Add,
        );
        assert_eq!(from_memory_block::<u128>(result.unwrap()), 8);
    }

    #[test]
    fn test_divide_by_zero() {
        // Must be reported, not panicked on: this used to be a `debug_assert`,
        // so release builds fell through to a real divide and crashed the
        // worker thread evaluating the row.
        let err = perform_math_operation(
            to_le_bytes(5i32).into_slice(),
            to_le_bytes(0i32).into_slice(),
            &DbType::I32,
            &MathOperation::Divide,
        )
        .unwrap_err();
        assert_eq!(err, EvalError::DivisionByZero);
    }

    #[test]
    fn test_divide_by_zero_float() {
        let err = perform_math_operation(
            to_le_bytes(5f64).into_slice(),
            to_le_bytes(0f64).into_slice(),
            &DbType::F64,
            &MathOperation::Divide,
        )
        .unwrap_err();
        assert_eq!(err, EvalError::DivisionByZero);
    }

    #[test]
    fn test_root_by_zero() {
        let err = perform_math_operation(
            to_le_bytes(5i32).into_slice(),
            to_le_bytes(0i32).into_slice(),
            &DbType::I32,
            &MathOperation::Root,
        )
        .unwrap_err();
        assert_eq!(err, EvalError::RootByZero);
    }

    #[test]
    fn test_edge_cases() {
        // Overflow is reported in every build profile. The release profile sets
        // `overflow-checks = false`, so before this was checked explicitly the
        // same expression wrapped silently and returned a wrong answer.
        let err = perform_math_operation(
            to_le_bytes(i32::MAX).into_slice(),
            to_le_bytes(1i32).into_slice(),
            &DbType::I32,
            &MathOperation::Add,
        )
        .unwrap_err();
        assert_eq!(err, EvalError::ArithmeticOverflow);

        let err = perform_math_operation(
            to_le_bytes(i32::MIN).into_slice(),
            to_le_bytes(1i32).into_slice(),
            &DbType::I32,
            &MathOperation::Subtract,
        )
        .unwrap_err();
        assert_eq!(err, EvalError::ArithmeticOverflow);

        let err = perform_math_operation(
            to_le_bytes(u8::MAX).into_slice(),
            to_le_bytes(2u8).into_slice(),
            &DbType::U8,
            &MathOperation::Multiply,
        )
        .unwrap_err();
        assert_eq!(err, EvalError::ArithmeticOverflow);

        // Floats keep IEEE semantics rather than erroring.
        let ok = perform_math_operation(
            to_le_bytes(f64::MAX).into_slice(),
            to_le_bytes(f64::MAX).into_slice(),
            &DbType::F64,
            &MathOperation::Add,
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn test_unequal_length_inputs() {
        let input1_block = to_le_bytes(5i32);
        let input2_block = to_le_bytes(3i16);
        let input1 = input1_block.into_slice();
        let input2 = input2_block.into_slice();
        let err =
            perform_math_operation(input1, input2, &DbType::I32, &MathOperation::Add).unwrap_err();
        assert_eq!(err, EvalError::OperandSizeMismatch);
    }

    /// Arithmetic on a type that has none must be reported, not panicked: the
    /// type comes from a client's query, and a panic here takes down the worker
    /// thread evaluating the row.
    #[test]
    fn test_unsupported_type() {
        for db_type in [DbType::STRING, DbType::UUID, DbType::NULL, DbType::END] {
            let err = perform_math_operation(
                to_le_bytes(5i32).into_slice(),
                to_le_bytes(3i32).into_slice(),
                &db_type,
                &MathOperation::Add,
            )
            .unwrap_err();
            assert_eq!(err, EvalError::UnsupportedType, "for {:?}", db_type);
        }
    }
}
