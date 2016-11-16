extern crate timely;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;

fn main() {

    //let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();

    // initializes and runs a timely dataflow computation
    /*timely::execute_from_args(std::env::args().skip(2), move |computation| {
        let index = computation.index();
        computation.scoped(move |builder| {
            let (helper, cycle) = builder.loop_variable(iterations, 1);
            (0..1).take(if index == 0 { 1 } else { 0 })
                  .to_stream(builder)
                  .concat(&cycle)
                  .exchange(|&x| x)
                  .map_in_place(|x| *x += 1)
                  .connect_loop(helper);
        });
    }).unwrap();*/

    // let iterations = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();

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

        /*let mut input = computation.scoped(move |scope| {
            let (input, stream) = scope.new_input();
            let (helper, cycle) = scope.loop_variable(10, 1);
            stream.concat(&cycle)
                  .exchange(|&x| x)
                  .map(|x| x + 1)
                  .connect_loop(helper);
            input
        });

        let (mut input, probe) = computation.scoped(move |outer| {
            let (input, stream) = outer.new_input();
            let result = outer.scoped(move |inner| {

                // construct the same loop as before, but bind the result
                // so we can both connect the loop, and return its output.
                let (helper, cycle) = inner.loop_variable(iterations, 1);
                let result = inner.enter(&stream)
                                  .concat(&cycle)
                                  .exchange(|&x| x)
                                  .map(|x| x + 1);

                result.connect_loop(helper);
                result.leave()
            });

            (input, result.probe().0)...
        });*/

        // introduce data and watch!
        for round in 0..10 {
            input.send(round);
            //println!("Epoch: {}", input.epoch());
            input.advance_to(round + 1);
            while probe.le(&RootTimestamp::new(round)) {
                computation.step();
            }    
        }
    });
}