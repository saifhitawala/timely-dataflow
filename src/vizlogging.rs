//! Traits, implementation, and macros related to logging timely events in json.
//! The json output files are used by a viz web app.
extern crate time;

use std::cell::RefCell;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::prelude::*;

use ::Data;

use dataflow::scopes::root::Root;
use dataflow::operators::input::Handle;

use progress::timestamp::Timestamp;

use timely_communication::Allocate;

use abomonation::Abomonation;

/// Returns the value of an high resolution performance counter, in nanoseconds, rebased to be
/// roughly comparable to an unix timestamp.
/// Useful for comparing and merging logs from different machines (precision is limited by the
/// precision of the wall clock base; clock skew effects should be taken into consideration).
#[inline(always)]
pub fn get_precise_time_ns() -> u64 {
    time::precise_time_ns() as u64
}

thread_local!{
    /// Stores the worker index throughout the logging of all computational events
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
      
        let path_string : String = format!("logs/message.txt");
        let path : &str = &path_string[..];
        if path_exists(path) {
            let file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            let time = get_precise_time_ns();
            f.write_fmt(format_args!("{{ \"MessagesEvent\": {{ \
                         \"is_send\": {:?}, \
                         \"channel\": {:?}, \
                         \"source_worker_id\": {:?}, \
                         \"dest_worker_id\": {:?}, \
                         \"number_of_records\": {:?}, \
                         \"timestamp\": {:?} \
                   }} }}\n",
            message_event.is_send, message_event.channel, message_event.source_worker_id, message_event.dest_worker_id, message_event.number_of_records, time)).expect("Unable to write data");
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            let time = get_precise_time_ns();
            f.write_fmt(format_args!("{{ \"MessagesEvent\": {{ \
                         \"is_send\": {:?}, \
                         \"channel\": {:?}, \
                         \"source_worker_id\": {:?}, \
                         \"dest_worker_id\": {:?}, \
                         \"number_of_records\": {:?}, \
                         \"timestamp\": {:?} \
                   }} }}\n",
            message_event.is_send, message_event.channel, message_event.source_worker_id, message_event.dest_worker_id, message_event.number_of_records, time)).expect("Unable to write data");
        }

}

/// logs operator events
pub fn log_operator_info(operate_event: OperatesEvent) {

        let path_string : String = format!("logs/operate.txt");
        let path : &str = &path_string[..];
        if path_exists(path) {
            let file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            let time = get_precise_time_ns();
            worker_index.with(|wid| {
                if *wid.borrow() == 0 
                {
                    f.write_fmt(format_args!("{{ \"OperatesEvent\": {{ \
                                 \"id\": {:?}, \
                                 \"addr\": {:?}, \
                                 \"name\": {:?}, \
                                 \"timestamp\": {:?} \
                           }} }}\n",
                    operate_event.id, operate_event.addr, operate_event.name, time)).expect("Unable to write data");
                }
            });  
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            let time = get_precise_time_ns();
            worker_index.with(|wid| {
                if *wid.borrow() == 0 
                {
                    f.write_fmt(format_args!("{{ \"OperatesEvent\": {{ \
                                 \"id\": {:?}, \
                                 \"addr\": {:?}, \
                                 \"name\": {:?}, \
                                 \"timestamp\": {:?} \
                           }} }}\n",
                    operate_event.id, operate_event.addr, operate_event.name, time)).expect("Unable to write data");
                }
            }); 
        }

}

/// logs channel events
pub fn log_channel_info(channel_event: ChannelsEvent) {

        let path_string : String = format!("logs/operate.txt");
        let path : &str = &path_string[..];
        if path_exists(path) {
            let file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            let time = get_precise_time_ns();
            worker_index.with(|wid| {
                if *wid.borrow() == 0 
                {
                    f.write_fmt(format_args!("{{ \"ChannelsEvent\": {{ \
                                 \"id\": {:?}, \
                                 \"scope_addr\": {:?}, \
                                 \"source\": {:?}, \
                                 \"target\": {:?}, \
                                 \"timestamp\": {:?} \
                           }} }}\n",
                    channel_event.id, channel_event.scope_addr, vec![channel_event.source.0, channel_event.source.1], vec![channel_event.target.0, channel_event.target.1], time)).expect("Unable to write data");   
                }
            }); 
            
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            let time = get_precise_time_ns();
            worker_index.with(|wid| {
                if *wid.borrow() == 0 
                {
                    f.write_fmt(format_args!("{{ \"ChannelsEvent\": {{ \
                                 \"id\": {:?}, \
                                 \"scope_addr\": {:?}, \
                                 \"source\": {:?}, \
                                 \"target\": {:?}, \
                                 \"timestamp\": {:?} \
                           }} }}\n",
                    channel_event.id, channel_event.scope_addr, vec![channel_event.source.0, channel_event.source.1], vec![channel_event.target.0, channel_event.target.1], time)).expect("Unable to write data");   
                }
            });
        }    
    
}

/// gets the epoch
pub fn get_epoch<T: Timestamp+Ord, D: Data>(handle: &mut Handle<T, D>) {

        let path_string : String = format!("logs/message.txt");
        let path : &str = &path_string[..];
        if path_exists(path) {
            let file =
            OpenOptions::new()
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

            let mut f = BufWriter::new(file);
            let time = get_precise_time_ns();
            worker_index.with(|wid| {
                f.write_fmt(format_args!("{{ \"Epoch\":  {:?}, \
                    \"Worker_Id\": {:?}, \
                    \"Timestamp\": {:?} \
                     }}\n",
                handle.epoch(), *wid.borrow(), time)).expect("Unable to write data");
            });
        }
        else{
            let f = File::create(path).expect("Unable to create file");
            let mut f = BufWriter::new(f);
            let time = get_precise_time_ns();
            worker_index.with(|wid| {
                f.write_fmt(format_args!("{{ \"Epoch\":  {:?}, \
                    \"Worker_Id\": {:?}, \
                    \"Timestamp\": {:?} \
                     }}\n",
                handle.epoch(), *wid.borrow(), time)).expect("Unable to write data");
            });
        }
    
}

/// sets the index of a worker
pub fn set_index<A: Allocate>(root: &mut Root<A>){
    worker_index.with(|f| {
        *f.borrow_mut() = root.index();
    });
}