extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;

fn main() {

    // initializes and runs a timely dataflow computation
    timely::execute_from_args(std::env::args(), move |computation| {

        // create a new input, and inspect its output
        let (mut input, probe) = computation.scoped(move |scope| {
            let index = scope.index();
            let (input, stream) = scope.new_input();
            let probe = stream.exchange(|x| *x as u64)
                .inspect(move |x| println!("Initially:\t worker {}: \t\thello {}", index, x))
                .map(|x| x+1)
                .inspect(move |x| println!("Map:\t\t worker {}: \t\thello {}", index, x))
                .filter(|x| *x % 1 == 0)
                .inspect(move |x| println!("Filter:\t\t worker {}: \t\thello {}", index, x))
                .map_in_place(|x| *x += 1)
                .inspect(move |x| println!("Map_in_place:\t worker {}: \t\thello {}", index, x))
                .accumulate(0, |sum, data| { for &x in data.iter() { *sum += x; } })
                .inspect(move |x| println!("Accumulate:\t worker {}: \t\thello {}", index, x))
                .broadcast()
                .inspect(move |x| println!("Broadcast:\t worker {}: \t\thello {}", index, x))
                .map(|x| x+1)
                .inspect(move |x| println!("Map 2:\t\t worker {}: \t\thello {}", index, x))
                .inspect_batch(move |t,x| println!("Inspect_batch:\t Time: {:?}\t Records: {:?}", t, x.len()))
                .count()
                .probe().0;
            (input, probe)
        });

        // introduce data and watch!
        for round in 0..3 {
            input.send(round);
            input.advance_to(round + 1);
            while probe.le(&RootTimestamp::new(round)) {
                computation.step();
            }    
        }
    });
}