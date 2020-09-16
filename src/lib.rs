use std::{mem, ptr};

struct SpscQueue<V: Send + Sync> {
    buffer: *mut V,
    capacity: usize,
    capacity_mask: usize,
    // We implement it at the queue level as it's a common requirement and so that V doesn't have to
    // be a heavier enum with an end message variant.
    ended: bool,
    read_next: usize,
    write_next: usize,
}

unsafe impl<V: Send + Sync> Send for SpscQueue<V> {}

unsafe impl<V: Send + Sync> Sync for SpscQueue<V> {}

impl<V: Send + Sync> Drop for SpscQueue<V> {
    fn drop(&mut self) {
        unsafe {
            let _ = Vec::from_raw_parts(self.buffer, 0, self.capacity);
        };
    }
}

impl<V: Send + Sync> SpscQueue<V> {
    pub fn new(capacity_exponent: usize) -> SpscQueue<V> {
        assert!(capacity_exponent < mem::size_of::<usize>() * 8);
        let capacity = 1 << capacity_exponent;
        let mut vec = Vec::with_capacity(capacity);
        let ptr = vec.as_mut_ptr();
        mem::forget(vec);
        SpscQueue {
            buffer: ptr,
            capacity,
            capacity_mask: capacity - 1,
            ended: false,
            read_next: 0,
            write_next: 0,
        }
    }
}

// Producer owns the underlying queue and drops it when itself is released.
pub struct SpscQueueProducer<V: Send + Sync> {
    queue: *mut SpscQueue<V>,
}

impl<V: Send + Sync> Drop for SpscQueueProducer<V> {
    fn drop(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.queue);
        };
    }
}

impl<V: Send + Sync> SpscQueueProducer<V> {
    pub fn enqueue(&mut self, value: V) -> () {
        let queue = unsafe { &mut *self.queue };
        while queue.write_next >= queue.read_next + queue.capacity {
            // Wait for consumer to catch up.
        };
        unsafe { ptr::write(queue.buffer.offset((queue.write_next & queue.capacity_mask) as isize), value) };
        // Increment after setting buffer element.
        queue.write_next += 1;
    }

    pub fn finish(&mut self) -> () {
        let queue = unsafe { &mut *self.queue };
        queue.ended = true;
    }
}

pub enum MaybeDequeued<V> {
    Ended,
    None,
    Some(V),
}

pub struct SpscQueueConsumer<V: Send + Sync> {
    queue: *mut SpscQueue<V>,
}

unsafe impl<V: Send + Sync> Send for SpscQueueConsumer<V> {}

unsafe impl<V: Send + Sync> Sync for SpscQueueConsumer<V> {}

impl<V: Send + Sync> SpscQueueConsumer<V> {
    #[inline(always)]
    fn queue(&self) -> &SpscQueue<V> {
        unsafe { &*self.queue }
    }

    #[inline(always)]
    fn queue_mut(&self) -> &mut SpscQueue<V> {
        unsafe { &mut *self.queue }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let queue = self.queue();
        queue.read_next >= queue.write_next
    }

    pub fn maybe_dequeue(&mut self) -> MaybeDequeued<V> {
        if self.is_empty() {
            if self.queue().ended {
                return MaybeDequeued::Ended;
            };
            return MaybeDequeued::None;
        };
        let queue = self.queue_mut();
        let value = unsafe { ptr::read(queue.buffer.offset((queue.read_next & queue.capacity_mask) as isize)) };
        queue.read_next += 1;
        MaybeDequeued::Some(value)
    }

    pub fn dequeue(&mut self) -> Option<V> {
        loop {
            match self.maybe_dequeue() {
                // Wait for producer to provide values.
                MaybeDequeued::None => {}
                // We've caught up to the end.
                MaybeDequeued::Ended => return None,
                MaybeDequeued::Some(v) => return Some(v),
            };
        };
    }
}

pub fn create_spsc_queue<V: Send + Sync>(capacity_exponent: usize) -> (SpscQueueProducer<V>, SpscQueueConsumer<V>) {
    let queue = Box::into_raw(Box::new(SpscQueue::<V>::new(capacity_exponent)));
    (SpscQueueProducer { queue }, SpscQueueConsumer { queue })
}
