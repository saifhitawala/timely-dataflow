//! Traits, implementation, and macros related to logging timely events in json.
//! The json output files are used by a viz web app.

use std::cell::RefCell;
use std::io::Write;
use std::fs::File;

use ::Data;

use ::logging;

/// temp doc
pub fn log_operator_info(op_info: logging::OperatesEvent) {
    println!("{:?}", op_info);
}

/// temp doc
pub fn log_channel_info(channel_event: logging::ChannelsEvent) {
    println!("{:?}", channel_event);
}

/// temp doc
pub fn log_message_info(message_event: logging::MessagesEvent) {
    println!("{:?}", message_event);
}

/// temp doc
pub fn log_schedule_info(schedule_event: logging::ScheduleEvent) {
    println!("{:?}", schedule_event);
}