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

use dataflow::operators::input::Handle;

use std::io::prelude::*;

use abomonation::Abomonation;

use ::logging;

thread_local!{
    /// fuck the system
    pub static worker_index: RefCell<usize> = RefCell::new(1);
}

#[derive(Debug, Clone)]
/// Message send or receive event
pub struct MessagesEvent {
    /// `true` if send event, `false` if receive event.
    pub is_send: bool,
    /// Channel identifier
    pub channel: usize,
    /// Source worker index.
    pub source_worker_id: usize,
    /// Source worker index.
    pub dest_worker_id: usize,
    /// Number of typed records in the message.
    pub number_of_records: usize,
}

unsafe_abomonate!(MessagesEvent);

///checks if a path exists
pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}

/// temp doc
pub fn log_message_info(message_event: MessagesEvent) {
      
    unsafe{

        // let worker_index = get_worker_index();
        // println!("Index in log_message_info {}", worker_index);
        let path_String : String = format!("logs/message.txt");
        let path : &str = &path_String[..];
        if path_exists(path) {
            let mut file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            f.write_fmt(format_args!("{{ \"MessagesEvent\": {{ \
                         \"is_send\": {:?}, \
                         \"channel\": {:?}, \
                         \"source_worker_id\": {:?}, \
                         \"dest_worker_id\": {:?}, \
                         \"number_of_records\": {:?}  \
                   }} }}\n",
            message_event.is_send, message_event.channel, message_event.source_worker_id, message_event.dest_worker_id, message_event.number_of_records)).expect("Unable to write data");
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            f.write_fmt(format_args!("{{ \"MessagesEvent\": {{ \
                         \"is_send\": {:?}, \
                         \"channel\": {:?}, \
                         \"source_worker_id\": {:?}, \
                         \"dest_worker_id\": {:?}, \
                         \"number_of_records\": {:?}  \
                   }} }}\n",
            message_event.is_send, message_event.channel, message_event.source_worker_id, message_event.dest_worker_id, message_event.number_of_records)).expect("Unable to write data");
        
        }

    }
}

/// get the epoch
pub fn get_epoch<T: Timestamp+Ord, D: Data>(handle: &mut Handle<T, D>) {
    let path_String : String = format!("logs/message.txt");
    let path : &str = &path_String[..];
    if path_exists(path) {
        let mut file =
        OpenOptions::new()
        .write(true)
        .append(true)
        .open(path)
        .unwrap();

        let mut f = BufWriter::new(file);
        worker_index.with(|wid| {
            f.write_fmt(format_args!("{{ \"Epoch\":  {:?}, \
                \"Worker_Id\": {:?} \
                 }}\n",
            handle.epoch(), *wid.borrow())).expect("Unable to write data");
        });
    }
    else{
        let f = File::create(path).expect("Unable to create file");
        let mut f = BufWriter::new(f);
        worker_index.with(|wid| {
            f.write_fmt(format_args!("{{ \"Epoch\":  {:?}, \
                \"Worker_Id\": {:?} \
                 }}\n",
            handle.epoch(), *wid.borrow())).expect("Unable to write data");
        });
    }
}

/// get the index of a worker
pub fn set_index<A: Allocate>(root: &mut Root<A>){
    worker_index.with(|f| {
        //assert_eq!(*f.borrow(), 1);
        *f.borrow_mut() = root.index();
    });
}