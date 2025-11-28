mod connection;
mod control;
mod error;
mod msg;
mod service;
mod utils;

pub use self::{
    control::{
        ControlProtocolConnection, ControlProtocolConnectionBuilder,
        ControlProtocolConnectionError, ControlProtocolConnectionHandle, ControlProtocolService,
    },
    service::{ServiceProtocolConnection, ServiceProtocolConnectionBuilder},
};

const PROTOCOL_VERSION: u8 = 3;
