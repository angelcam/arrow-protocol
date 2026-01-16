# Arrow Protocol

The main purpose of the Arrow Protocol is to provide access to local services
and devices like IP cameras and NVRs from Angelcam Cloud. These services could
normally be behind a NAT or a firewall making it difficult to access them
directly. Each Arrow client is essentially a TCP proxy that forwards traffic
between Angelcam Cloud and local services.

The protocol is designed to be efficient and lightweight, minimizing the
overhead introduced by the protocol itself. It is also designed to be secure,
using encryption and authentication mechanisms to protect the data being
transmitted and the target network from unauthorized access. All traffic is
encrypted using TLS v1.2 or higher.

This repository contains documentation of the Arrow Protocol and also reference
implementations of the Arrow Protocol v2 and v3 in Rust. There is also a
[reference client implementation](https://github.com/angelcam/arrow-client) of
the Arrow Protocol v3 also in Rust.

## Protocol versions

* [Arrow Protocol v2](docs/v2.md) (deprecated)
* [Arrow Protocol v3](docs/v3.md)
