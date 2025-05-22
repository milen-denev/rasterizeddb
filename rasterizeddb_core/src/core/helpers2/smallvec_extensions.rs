use smallvec::{Array, SmallVec};
use core::ptr::copy;

/// Extension trait that adds push_back and pop_front methods to SmallVec
pub trait SmallVecExtensions<A: Array> {
    /// Adds an element to the back of the vector (equivalent to push)
    fn push_back(&mut self, value: A::Item);
    
    /// Removes the first element from the vector and returns it, or None if the vector is empty
    fn pop_front(&mut self) -> Option<A::Item>;
}

impl<A: Array> SmallVecExtensions<A> for SmallVec<A> {
    #[inline]
    fn push_back(&mut self, value: A::Item) {
        self.push(value)
    }

    #[inline]
    fn pop_front(&mut self) -> Option<A::Item> {
        if self.is_empty() {
            None
        } else {
            // Read the first element
            let value = unsafe { self.as_ptr().read() };
            
            // Shift all remaining elements to the left
            unsafe {
                let ptr = self.as_mut_ptr();
                let len = self.len();
                copy(ptr.add(1), ptr, len - 1);
                self.set_len(len - 1);
            }
            Some(value)
        }
    }
}