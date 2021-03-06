extern crate timely;

use std::net::TcpListener;
use timely::dataflow::Scope;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::capture::{EventReader, Replay};

fn main() {
    timely::execute(timely::Configuration::Thread, |computation| {
        let list = TcpListener::bind("127.0.0.1:8000").unwrap();
        let recv = list.incoming().next().unwrap().unwrap();

        computation.scoped::<u64,_,_>(|scope2| {
            EventReader::<_,u64,_>::new(recv)
                .replay_into(scope2)
                .inspect(|x| println!("replayed: {:?}", x));
        })
    }).unwrap(); // asserts error-free execution
}
