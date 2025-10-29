use core::ptr::copy;
use smallvec::{Array, SmallVec};

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

    /// Extend the SmallVec by cloning all elements from a slice (same semantics as Vec::extend_from_slice)
    fn extend_from_slice(&mut self, other: &[A::Item])
    where
        A::Item: Clone;
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

    fn extend_from_slice(&mut self, other: &[A::Item])
    where
        A::Item: Clone,
    {
        if other.is_empty() {
            return;
        }

        // Reserve additional space for the elements we're going to add.
        // SmallVec::reserve expects the additional number of elements.
        self.reserve(other.len());

        // Clone each element from the slice into the SmallVec.
        for item in other {
            self.push(item.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SmallVecExtensions;
    use smallvec::SmallVec;

    #[test]
    fn push_back_and_pop_front_basic_inline() {
        let mut sv: SmallVec<[i32; 4]> = SmallVec::new();

        sv.push_back(1);
        sv.push_back(2);
        sv.push_back(3);

        assert_eq!(sv.len(), 3);
        assert_eq!(sv.pop_front(), Some(1));
        assert_eq!(sv.as_slice(), &[2, 3]);
        assert_eq!(sv.len(), 2);
    }

    #[test]
    fn pop_front_empty_returns_none() {
        let mut sv: SmallVec<[i32; 2]> = SmallVec::new();
        assert_eq!(sv.pop_front(), None);
        assert!(sv.is_empty());
    }

    #[test]
    fn pop_front_until_partial_on_heap_backed() {
        // Force heap allocation by exceeding inline capacity (4)
        let mut sv: SmallVec<[i32; 4]> = SmallVec::new();
        for i in 0..10 {
            sv.push_back(i);
        }

        assert_eq!(sv.len(), 10);
        assert_eq!(sv.pop_front(), Some(0));
        assert_eq!(sv.pop_front(), Some(1));
        assert_eq!(sv.pop_front(), Some(2));
        assert_eq!(sv.as_slice(), &[3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(sv.len(), 7);
    }

    #[test]
    fn splice_delete_only_middle_range() {
        let mut sv: SmallVec<[i32; 8]> = SmallVec::from_buf([1, 2, 3, 4, 5, 0, 0, 0]);
        // Truncate to actual initial len 5
        sv.truncate(5);

        // Remove 3 elements starting at index 1: remove 2,3,4
        sv.splice(1, 3, std::iter::empty());
        assert_eq!(sv.as_slice(), &[1, 5]);
    }

    #[test]
    fn splice_insert_only_no_delete() {
        let mut sv: SmallVec<[i32; 4]> = SmallVec::from_slice(&[1, 2]);
        sv.splice(1, 0, [9, 9]);
        assert_eq!(sv.as_slice(), &[1, 9, 9, 2]);
    }

    #[test]
    fn splice_replace_with_more_items() {
        let mut sv: SmallVec<[i32; 8]> = SmallVec::from_slice(&[1, 2, 3, 4, 5]);
        // Replace [2,3] with [8,9,10]
        sv.splice(1, 2, [8, 9, 10]);
        assert_eq!(sv.as_slice(), &[1, 8, 9, 10, 4, 5]);
    }

    #[test]
    fn extend_from_slice_with_strings_clone_items() {
        let mut sv: SmallVec<[String; 2]> = SmallVec::new();
        let other = ["a".to_string(), "b".to_string(), "c".to_string()];
        sv.extend_from_slice(&other);

        assert_eq!(sv.len(), 3);
        assert_eq!(sv.as_slice(), &["a", "b", "c"]);

        // Ensure original slice is intact (cloned, not moved)
        assert_eq!(other[0], "a");
        assert_eq!(other[1], "b");
        assert_eq!(other[2], "c");
    }
}
