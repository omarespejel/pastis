#![cfg(feature = "bigdecimal")]

use crate::{Context, SizeOf};
use bigdecimal::BigDecimal;
use core::mem::size_of;

impl SizeOf for BigDecimal {
    fn size_of_children(&self, context: &mut Context) {
        // BigDecimal stores its coefficient in a BigInt; use limb count instead of
        // decimal digit count to avoid significant overestimation.
        let (coefficient, _) = self.as_bigint_and_exponent();
        let limbs = coefficient.iter_u64_digits().len();
        if limbs != 0 {
            context
                .add_arraylike(limbs, size_of::<u64>())
                .add_distinct_allocation();
        }
    }
}
