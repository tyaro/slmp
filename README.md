# Seamless Message Protocol (SLMP) for Rust
This library provides SLMP client to access the PLCs of Mitsubishi Electric

> [!NOTE]
> **This is a fork of [DigitalServo/slmp-rs](https://github.com/DigitalServo/slmp-rs).**
> Starting with **v0.2.0** it diverges from upstream: the fallible API now
> returns a structured [`SlmpError`](#error-handling) instead of
> `std::io::Result<T>`, so callers can tell a device-rejected request (a
> non-zero SLMP end code, recoverable) from a framing failure (fatal, needs a
> reconnect) **without parsing error message text**. See
> [Error Handling](#error-handling) for the full API and migration notes.


## Get Started
First of all, **You should enable SLMP communication (binary mode) and open a port** using GX Works 2/3.

This library supports the connection to MELSEC-Q and MELSEC iQ-R PLCs, using a 4E frame.
You can pass a connection property with `new()` and try to connect with `connect()`.

```rust
use slmp::{SLMPClient, SLMP4EConnectionProps};

#[tokio::main]
async fn main() {

    const conn_props: SLMP4EConnectionProps = SLMP4EConnectionProps {...};

    let mut client = SLMPClient::new(conn_props);
    client.connect().await.unwrap();

    ...
    
    client.close().await;
}
```

## Access Method
SLMP provides roughly 5 categories; 
- [x] Device access
- [ ] Label access
- [ ] Buffer-memory access
- [x] Unit control
- [ ] File control
  
This library supports **device access** and **unit control** methods.

### Device Control
This library enable you to use
- [x] Bulk read/write
- [x] Random read/write
- [x] Block read/write
- [x] Monitor register/read

and primitive types
- [x] bool
- [x] [bool; 16] (Word-size access)
- [x] u16
- [x] i16
- [x] u32
- [x] i32
- [x] f32
- [x] f64
- [x] String

The samples of those methods are prepared in `/examples`:
```bash
cargo r --example bulk_access 
cargo r --example random_access 
cargo r --example block_access 
cargo r --example monitor_read
```

### Unit Control
This library supports
- [x] Remote run
- [x]  Remote stop
- [x]  Remote pause
- [x]  Remote latch clear
- [x]  Remote reset *
- [x]  Get cpu type
- [x]  Remote unlock
- [x]  Remote lock
- [x]  Echo
- [ ]  Error Clear (for serial communication unit)

There are restrictions on use of remote reset.
Please check the document from Mitsubishi Electric.

The sample is prepared in `/examples`:
```bash
cargo r --example unit_control
```

## Error Handling
All fallible `SLMPClient` and `SLMPConnectionManager` methods return
`SlmpResult<T>` (an alias for `Result<T, SlmpError>`). `SlmpError` implements
`std::fmt::Display` and `std::error::Error`, so it works with `?` into
`Box<dyn std::error::Error>` and any `thiserror`/`anyhow`-style stack.

```rust
pub type SlmpResult<T> = Result<T, SlmpError>;

pub enum SlmpError {
    /// A complete, length-consistent frame arrived but the SLMP end code was
    /// non-zero: the device rejected this request. The byte stream is still
    /// aligned to request boundaries, so the caller MAY keep the connection
    /// and treat only this request as failed (recoverable, per-request).
    Device { end_code: u16 },
    /// The response structure itself is corrupt (bad length / bad fixed field /
    /// echo mismatch). The stream may be desynchronized; the caller SHOULD
    /// drop and reopen the connection (fatal).
    Framing(FramingError),
    /// A send / receive / connect deadline elapsed.
    Timeout,
    /// The stream is not connected.
    NotConnected,
    /// Any other transport / IO failure (connection refused, reset, broken
    /// pipe, EOF, DNS / address resolution, ...).
    Io(std::io::Error),
}

pub enum FramingError {
    /// Frame shorter than the minimum 4E response (fixed header + end code).
    ShortFrame { len: usize, min: usize },
    /// Declared data-block length disagrees with the bytes actually received.
    LengthMismatch { declared: usize, actual: usize },
    /// A fixed header field held an unexpected value.
    UnexpectedField { field: &'static str },
    /// An echo response body did not match the payload that was sent.
    EchoMismatch,
}

/// Symbolic name for a non-zero SLMP end code (e.g. `0xC059 => "WrongCommand"`),
/// or `"Unknown Error"`.
pub fn end_code_name(code: u16) -> &'static str;
```

The key distinction is **`Device` vs `Framing`**: a non-zero end code means the
device answered a well-formed request and rejected it, so it is safe to retry
that single request on the same connection; a `Framing` error means the wire
framing is broken and the connection should be torn down.

```rust
match client.bulk_read(start, num, DataType::U16).await {
    Ok(values) => { /* ... */ }
    Err(SlmpError::Device { end_code }) => {
        // Recoverable: the PLC rejected this request. Log and continue.
        eprintln!("device rejected request: {} (0x{:04X})", slmp::end_code_name(end_code), end_code);
    }
    Err(SlmpError::Timeout) | Err(SlmpError::NotConnected)
    | Err(SlmpError::Framing(_)) | Err(SlmpError::Io(_)) => {
        // Fatal for this connection: reconnect before retrying.
        client.connect().await?;
    }
}
```

Additional guarantees:
- **Frame length is validated before the end code is read.** `validate_response`
  rejects any frame shorter than the fixed header + end code (`FramingError::ShortFrame`)
  and any length-field mismatch (`FramingError::LengthMismatch`) *before* it
  inspects the end code. This upholds the invariant that "reaching the end code
  means a complete frame arrived" — the basis for treating `Device` as
  non-fatal — and closes a latent out-of-bounds panic on 13/14-byte frames.
- `From<std::io::Error> for SlmpError` maps `ErrorKind::TimedOut -> Timeout` and
  `ErrorKind::NotConnected -> NotConnected`; everything else becomes `Io`.

### Migrating from 0.1.x (upstream)
`0.2.0` is a **breaking change**. Method signatures changed from
`std::io::Result<T>` to `SlmpResult<T>`. In practice:
- Code that used `?` into `Box<dyn std::error::Error>` (as in the examples)
  keeps compiling unchanged.
- Code that matched on `std::io::ErrorKind` or parsed the old
  `"SLMP Returns Error: {name} (0x{code:X})"` message string must switch to
  matching `SlmpError` / `FramingError` variants.
- For `SLMPConnectionManager::connect` / `operate_worker`, the closure/future
  output type changes from `std::io::Result<T>` to `SlmpResult<T>`.

## Debugging Proxy
To check transferred data between a client and server, you can use a debugging-proxy server.
```bash
cargo r --example debugging_proxy
```
This could be used by setting IP/port of a proxy server on `SLMP4EConnectionProps` instead of setting those of a SLMP server.

## Multi-PLC Connection
`SLMPConnectionManager` allows you to connect a client to multi PLCs.
You can give a cyclic task to each connection.

```rust
use slmp::{CPU, SLMP4EConnectionProps, SLMPConnectionManager};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let manager = SLMPConnectionManager::new();

    const conn_props_1: SLMP4EConnectionProps = SLMP4EConnectionProps {...};
    const conn_props_2: SLMP4EConnectionProps = SLMP4EConnectionProps {...};

    let cyclic_task = async |data| {
        for x in data {
            println!("{:?}", x);
        }
        println!();
        Ok(())
    };

    manager.connect(conn_props_1, cyclic_task).await?;
    manager.connect(conn_props_2, cyclic_task).await?;
    
    ...

    manager.disconnect(conn_props_1).await?;
    manager.disconnect(conn_props_2).await?;

    Ok(())
}
```

The sample of cyclic read is prepared in `/examples`:
```bash
cargo r --example cyclic_read
```

> [!CAUTION]
> The SLMP protocol features a concise presentation layer without any encryption, and it allows device modifications, file operations, and changes to CPU operation settings without any authentication.
>
> The following vulnerabilities have been registered with CISA.
> - CVE-2020-5594: Cleartext Transmission of Sensitive Information
> - CVE-2020-16226: Impersonations of a Legitimate Device by a Malicious Actor
> - CVE-2023-4699: Arbitrary Command Execution
> - CVE-2025-7405: Missing Authentication for Critical Function
> - CVE-2025-7731: Cleartext Transmission of Sensitive Information
> 
> In response to the above reports, Mitsubishi Electric has implemented the following countermeasures (as stated in advisory 2025-08-28):
> - Use a virtual private network (VPN) or similar technology to encrypt SLMP communications.
> - Restrict physical access to the LAN to which the affected products are connected.
> 
> (Note: No firmware fix is planned for this vulnerability.)
> 
> It should be noted that improper use of SLMP carries significant risks, which allow attacks like man-in-the-middle (MitM), impersonation/spoofing, denial-of-service (DoS).
> Please use it with caution.
