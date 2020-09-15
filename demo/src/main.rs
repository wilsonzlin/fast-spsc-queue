use std::thread;
use spsc_queue::create_spsc_queue;

fn main() {
    let (mut producer, mut consumer) = create_spsc_queue::<String>(3);

    let child = thread::spawn(move || {
        while let Some(msg) = consumer.dequeue() {
            println!("Child received {}", msg);
        };
    });

    for i in 0..60 {
        producer.enqueue(i.to_string());
    };
    producer.finish();

    let _ = child.join();
}
