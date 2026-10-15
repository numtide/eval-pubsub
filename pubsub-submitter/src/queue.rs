use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

pub trait Keyable {
    type Key;

    fn key(&self) -> Self::Key;
}

pub struct Queue<T, K> {
    deque: VecDeque<Option<T>>,
    map: HashMap<K, usize>,
    max: usize,
    generation: usize,
}

impl<T> Queue<T, T::Key>
where
    T: Keyable,
    T::Key: Eq,
    T::Key: Hash,
{
    pub fn new(max: usize) -> Self {
        Self {
            deque: VecDeque::new(),
            map: HashMap::new(),
            max,
            generation: 0,
        }
    }

    pub fn push(&mut self, item: T) -> Option<T> {
        if self.deque.len() < self.max {
            self.map.insert(item.key(), self.generation);
            self.deque.push_front(Some(item));
            self.generation += 1;
            None
        } else {
            Some(item)
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        loop {
            // we skip over any removed elements, this makes this function O(n) in the worst case
            match self.deque.pop_back() {
                // there was an element, but it's been removed
                Some(None) => {}
                // there is an element, return it
                Some(Some(item)) => break Some(item),
                // we reached the end of the deque
                None => break None,
            }
        }
    }

    pub fn remove(&mut self, item: T) -> Option<T> {
        let key = item.key();

        match self.map.get(&key) {
            Some(generation) => {
                let index = self.generation - generation;
                self.deque.push_front(None);
                self.deque.swap(index, 0);
                self.map.remove(&key);
                self.deque
                    .pop_front()
                    .expect("we just pushed on an item, where did it go?")
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct Item(i32);
    impl Keyable for Item {
        type Key = i32;

        fn key(&self) -> Self::Key {
            self.0
        }
    }

    #[test]
    fn empty() {
        let mut q: Queue<Item, i32> = Queue::new(128);

        assert_eq!(q.pop(), None);
    }

    #[test]
    fn push_2_pop_2() {
        let mut q = Queue::new(128);

        q.push(Item(0));
        q.push(Item(1));

        assert_eq!(q.pop(), Some(Item(0)));
        assert_eq!(q.pop(), Some(Item(1)));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn push_3_remove_2nd_pop_2() {
        let mut q = Queue::new(128);

        q.push(Item(0));
        q.push(Item(1));
        q.push(Item(2));

        q.remove(Item(1));

        assert_eq!(q.pop(), Some(Item(0)));
        assert_eq!(q.pop(), Some(Item(2)));
        assert_eq!(q.pop(), None);
    }
}
