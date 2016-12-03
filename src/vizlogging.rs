//! Traits, implementation, and macros related to logging timely events in json.
//! The json output files are used by a viz web app.
extern crate time;

use std::cell::RefCell;
use std::fmt::Arguments;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::prelude::*;

use ::Data;
use ::logging::OperatesEvent;
use ::logging::ChannelsEvent;
use ::logging::MessagesEvent;
use dataflow::scopes::root::Root;
use timely_communication::Allocate;

thread_local! {
    /// worker index used for the viz logging feature.
    pub static worker: RefCell<usize> = RefCell::new(0);
}

/// set the worker index, called at initialization
pub fn set_worker_index<A: Allocate>(root: &mut Root<A>){
    worker.with(|index| {
        *index.borrow_mut() = root.index();
    });
}

/// enum used to match different events
pub enum Events {
    /// from ::logging both send and recv
    Msg(MessagesEvent, String),
    /// from ::logging (vertex)
    Op(OperatesEvent),
    /// from ::logging (edge)
    Ch(ChannelsEvent),
}

/// logs messages events for both sending and receiving ends.
pub fn log_messages_event(msg_event: MessagesEvent, time: String) {
    if cfg!(feature = "vizlogging") {   
        worker.with(|index| {
            let path = &format!("vizlogs/messages-{}.log", *index.borrow());
            log_event(path, Events::Msg(msg_event, time));
        });
    }
}

/// log operates creation event
pub fn log_operates_event(op_event: OperatesEvent) {
    if cfg!(feature = "vizlogging") {   
        worker.with(|index| {
            let path = &format!("vizlogs/topology-{}.log", *index.borrow());
            log_event(path, Events::Op(op_event));
        });
    }
}

/// log channels creation event
pub fn log_channels_event(ch_event: ChannelsEvent) {
    if cfg!(feature = "vizlogging") {   
        worker.with(|index| {
            let path = &format!("vizlogs/topology-{}.log", *index.borrow());
            log_event(path, Events::Ch(ch_event));
        });
    }
}

/// logs events and called by specialized log functions
pub fn log_event(path: &str, event: Events) {

    let file = if fs::metadata(path).is_ok() {
        OpenOptions::new().write(true).append(true)
                                        .open(path).unwrap()
    } else {
        File::create(path).expect("Unable to create log file") 
    };

    let mut buf_writer = BufWriter::new(file);

    match event {
        Events::Msg(e, t)
            => buf_writer.write_fmt(format_args!(
                  "{{ \"MessagesEvent\": \
                    {{\
                       \"is_send\": {:?}, \"channel\": {:?}, \"source\": {:?}, \
                       \"target\": {:?}, \"length\": {:?}, \"time\": {:?} \
                    }} \
                   }}\n",
                   e.is_send, e.channel, e.source,
                   e.target, e.length, t
                  )).expect("Unable to write to log file"),

        Events::Op(e)
            => buf_writer.write_fmt(format_args!(
                  "{{ \"OperatesEvent\": \
                    {{\
                      \"id\": {:?}, \"addr\": {:?}, \"name\": {:?} \
                    }} \
                   }}\n",
                   e.id, e.addr, e.name
                  )).expect("Unable to write to log file"),

        Events::Ch(e)
            => buf_writer.write_fmt(format_args!(
                  "{{ \"ChannelsEvent\": \
                    {{\
                      \"id\": {:?}, \"scope_addr\": {:?}, \
                      \"source\": {:?}, \"target\": {:?} \
                    }} \
                   }}\n",
                   e.id, e.scope_addr,
                   vec![e.source.0, e.source.1],
                   vec![e.target.0, e.target.1]
                  )).expect("Unable to write to log file"),
    }
}
