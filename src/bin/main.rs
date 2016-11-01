extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;

fn main() {
    // initializes and runs a timely dataflow computation
    timely::execute_from_args(std::env::args(), |computation| {

        // create a new input, and inspect its output
        let (mut input, probe) = computation.scoped(move |scope| {
            let index = scope.index();
            let (input, stream) = scope.new_input();
            let probe = stream.exchange(|x| *x as u64)
                .map(|x| vec![x, x+1, x+2])
                .map(|x| x[0]*x[1]*x[2])
                .inspect(move |x| println!("worker {}: \thello {}", index, x))
                .probe().0;
            (input, probe)
        });

        // introduce data and watch!
        for round in 0..3 {
            input.send(round);
            println!("Epoch: {}", input.epoch());
            input.advance_to(round + 1);
            while probe.le(&RootTimestamp::new(round)) {
                computation.step();
            }    
        }
    });
}