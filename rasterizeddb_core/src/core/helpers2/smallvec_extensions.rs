use smallvec::{Array, SmallVec};
use core::ptr::copy;

/// Extension trait that adds push_back and pop_front methods to SmallVec
pub trait SmallVecExtensions<A: Array> {
    /// Adds an element to the back of the vector (equivalent to push)
    fn push_back(&mut self, value: A::Item);
    
    /// Removes the first element from the vector and returns it, or None if the vector is empty
    fn pop_front(&mut self) -> Option<A::Item>;

    /// Simple splice: removes elements in range and inserts new elements at that position
    fn splice<I>(&mut self, start: usize, delete_count: usize, items: I)
    where
        I: IntoIterator<Item = A::Item>;
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

    fn splice<I>(&mut self, start: usize, delete_count: usize, items: I)
    where
        I: IntoIterator<Item = A::Item>,
    {
        let len = self.len();
        debug_assert!(start <= len, "start index out of bounds");
        
        let end = (start + delete_count).min(len);
        let actual_delete_count = end - start;
        
        // Collect new items
        let new_items: SmallVec<A> = items.into_iter().collect();
        let insert_count = new_items.len();
        
        // Calculate new length and reserve space if needed
        let new_len = len - actual_delete_count + insert_count;
        if new_len > self.capacity() {
            self.reserve(new_len - self.capacity());
        }
        
        unsafe {
            let ptr = self.as_mut_ptr();
            
            // Move elements after the deleted range
            if actual_delete_count != insert_count {
                let src = ptr.add(end);
                let dst = ptr.add(start + insert_count);
                let move_count = len - end;
                copy(src, dst, move_count);
            }
            
            // Insert new elements
            if insert_count > 0 {
                let src_ptr = new_items.as_ptr();
                let dst_ptr = ptr.add(start);
                copy(src_ptr, dst_ptr, insert_count);
            }
            
            self.set_len(new_len);
        }
        
        // Prevent new_items from dropping
        core::mem::forget(new_items);
    }
}