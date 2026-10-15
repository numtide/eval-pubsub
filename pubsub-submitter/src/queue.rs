use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::{RwLock, mpsc};

pub trait Keyable {
    type Key;

    fn key(&self) -> Self::Key;
}

#[derive(Debug)]
pub struct Sender<T, K> {
    sender: mpsc::Sender<T>,
    live_set: Arc<RwLock<HashSet<K>>>,
}

#[derive(Debug)]
pub struct Receiver<T, K> {
    receiver: mpsc::Receiver<T>,
    live_set: Arc<RwLock<HashSet<K>>>,
}

pub fn new<T, K>(max: usize) -> (Sender<T, K>, Receiver<T, K>) {
    let (sender, receiver) = mpsc::channel(max);
    let live_set = Arc::new(RwLock::new(HashSet::new()));

    (
        Sender {
            sender,
            live_set: live_set.clone(),
        },
        Receiver { receiver, live_set },
    )
}

impl<T> Sender<T, T::Key>
where
    T: Keyable,
    T::Key: Eq,
    T::Key: Hash,
{
    pub async fn push(&mut self, item: T) -> Option<T> {
        let key = item.key();
        match self.sender.try_send(item) {
            Ok(_) => {
                self.live_set.write().await.insert(key);
                None
            }
            Err(TrySendError::Full(item)) => Some(item),
            Err(TrySendError::Closed(item)) => Some(item),
        }
    }

    pub async fn remove(&mut self, item: T) -> bool {
        let key = item.key();
        self.live_set.write().await.remove(&key)
    }
}

impl<T> Receiver<T, T::Key>
where
    T: Keyable,
    T::Key: Eq,
    T::Key: Hash,
{
    pub async fn try_pop(&mut self) -> Option<T> {
        loop {
            // we skip over any removed elements, this makes this function O(n) in the worst case
            match self.receiver.try_recv() {
                Ok(item) => {
                    if self.live_set.write().await.remove(&item.key()) {
                        break Some(item);
                    }
                }
                Err(TryRecvError::Empty) => break None,
                Err(TryRecvError::Disconnected) => break None,
            }
        }
    }

    pub async fn pop(&mut self) -> Option<T> {
        loop {
            // we skip over any removed elements, this makes this function O(n) in the worst case
            match self.receiver.recv().await {
                Some(item) => {
                    if self.live_set.write().await.remove(&item.key()) {
                        break Some(item);
                    }
                }
                None => break None,
            }
        }
    }

    pub fn is_empty(&mut self) -> bool {
        self.receiver.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct Item(i32);
    impl Keyable for Item {
        type Key = i32;

        fn key(&self) -> Self::Key {
            self.0
        }
    }

    #[tokio::test]
    async fn empty() {
        let (_sender, mut receiver): (Sender<Item, _>, Receiver<Item, _>) = new(128);

        assert_eq!(receiver.try_pop().await, None);
    }

    #[tokio::test]
    async fn push_2_pop_2() {
        let (mut sender, mut receiver) = new(128);

        sender.push(Item(0)).await;
        sender.push(Item(1)).await;

        assert_eq!(receiver.try_pop().await, Some(Item(0)));
        assert_eq!(receiver.try_pop().await, Some(Item(1)));
        assert_eq!(receiver.try_pop().await, None);
    }

    #[tokio::test]
    async fn push_3_remove_2nd_pop_2() {
        let (mut sender, mut receiver) = new(128);

        sender.push(Item(0)).await;
        sender.push(Item(1)).await;
        sender.push(Item(2)).await;

        sender.remove(Item(1)).await;

        assert_eq!(receiver.try_pop().await, Some(Item(0)));
        assert_eq!(receiver.try_pop().await, Some(Item(2)));
        assert_eq!(receiver.try_pop().await, None);
    }
}
