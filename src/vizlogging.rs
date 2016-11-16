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

#[derive(Debug, Clone)]
/// The creation of an `Operate` implementor.
pub struct OperatesEvent {
    /// Worker-unique identifier for the operator.
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// A helpful name.
    pub name: String,
}

unsafe_abomonate!(OperatesEvent : addr, name);

#[derive(Debug, Clone)]
/// The creation of a channel between operators.
pub struct ChannelsEvent {
    /// Worker-unique identifier for the channel
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub scope_addr: Vec<usize>,
    /// Source descriptor, indicating operator index and output port.
    pub source: (usize, usize),
    /// Target descriptor, indicating operator index and input port.
    pub target: (usize, usize),
}

unsafe_abomonate!(ChannelsEvent : id, scope_addr, source, target);

///checks if a path exists
pub fn path_exists(path: &str) -> bool {
    fs::metadata(path).is_ok()
}

/// logs message events
pub fn log_message_info(message_event: MessagesEvent) {
      
    unsafe{

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

/// logs operator events
pub fn log_operator_info(operate_event: OperatesEvent) {

    unsafe{

        let path_String : String = format!("logs/operate.txt");
        let path : &str = &path_String[..];
        if path_exists(path) {
            let mut file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            f.write_fmt(format_args!("{{ \"OperatesEvent\": {{ \
                         \"id\": {:?}, \
                         \"addr\": {:?}, \
                         \"name\": {:?} \
                   }} }}\n",
            operate_event.id, operate_event.addr, operate_event.name)).expect("Unable to write data");
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            f.write_fmt(format_args!("{{ \"OperatesEvent\": {{ \
                         \"id\": {:?}, \
                         \"addr\": {:?}, \
                         \"name\": {:?} \
                   }} }}\n",
            operate_event.id, operate_event.addr, operate_event.name)).expect("Unable to write data");
        }

    }
}

/// logs channel events
pub fn log_channel_info(channel_event: ChannelsEvent) {

    unsafe{

        let path_String : String = format!("logs/operate.txt");
        let path : &str = &path_String[..];
        if path_exists(path) {
            let mut file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            f.write_fmt(format_args!("{{ \"ChannelsEvent\": {{ \
                         \"id\": {:?}, \
                         \"scope_addr\": {:?}, \
                         \"source\": {:?}, \
                         \"target\": {:?} \
                   }} }}\n",
            channel_event.id, channel_event.scope_addr, channel_event.source, channel_event.target)).expect("Unable to write data");
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            f.write_fmt(format_args!("{{ \"ChannelsEvent\": {{ \
                         \"id\": {:?}, \
                         \"scope_addr\": {:?}, \
                         \"source\": {:?}, \
                         \"target\": {:?} \
                   }} }}\n",
            channel_event.id, channel_event.scope_addr, channel_event.source, channel_event.target)).expect("Unable to write data");
        }

    }
}

/// get the epoch
pub fn get_epoch<T: Timestamp+Ord, D: Data>(handle: &mut Handle<T, D>) {

    unsafe{
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
}

/// get the index of a worker
pub fn set_index<A: Allocate>(root: &mut Root<A>){
    worker_index.with(|f| {
        //assert_eq!(*f.borrow(), 1);
        *f.borrow_mut() = root.index();
    });
}