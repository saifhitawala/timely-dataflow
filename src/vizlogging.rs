//! Traits, implementation, and macros related to logging timely events in json.
//! The json output files are used by a viz web app.
extern crate time;

use std::cell::RefCell;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufWriter};

use ::Data;

use std::fmt;
use std::fmt::Debug;

use timely_communication::Allocate;
use ::progress::timestamp::RootTimestamp;
use ::progress::nested::product::Product;

use dataflow::scopes::root::Root;
use dataflow::Scope;
use dataflow::operators::capture::{EventWriter, Event, EventPusher};

use progress::count_map::CountMap;
use progress::nested::subgraph::{Source, Target};
use progress::{Timestamp, Operate, Antichain};

use std::io::prelude::*;

use abomonation::Abomonation;

use ::logging;

///checks if a path exists
pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}

/// temp doc
pub fn log_message_info(message_event: logging::MessagesEvent) {
      
    unsafe{

        let path_String : String = format!("logs/message-{}.txt", worker_index);
        let path : &str = &path_String[..];
        if path_exists(path) {
            let mut file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            f.write_fmt(format_args!("{:?}\n", message_event)).expect("Unable to write data");
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            f.write_fmt(format_args!("{:?}\n", message_event)).expect("Unable to write data");
        }

    }
}

/// get the index of a worker
pub fn get_index<A: Allocate>(root: &mut Root<A>){
    //println!("Index of worker: {}\n", root.index());
    unsafe{
        static mut worker_index : usize = root.index();  
    } 
}