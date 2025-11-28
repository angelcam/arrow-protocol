mod connection;
mod control;
mod error;
mod msg;
mod service;
mod utils;

pub use self::{
    control::{
        ControlProtocolConnection, ControlProtocolConnectionBuilder,
        ControlProtocolConnectionError, ControlProtocolConnectionHandle, ControlProtocolHandshake,
        ControlProtocolService,
    },
    service::{
        ServiceProtocolConnection, ServiceProtocolConnectionBuilder, ServiceProtocolHandshake,
    },
};

const PROTOCOL_VERSION: u8 = 3;
