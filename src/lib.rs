mod commands;
mod data;
mod device;
mod manager;
mod monitor;


use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream};
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use serde::{Deserialize, Serialize};

use crate::commands::device_access::{read::*, write::*};
use crate::commands::unit_control;

use device::DeviceSize;

// Public
pub use data::{DataType, TypedData, string::{PLCString, PLCSTRING_QUERY_SPLITTER}};
pub use device::{AccessType, Device, DeviceType, DeviceData, DeviceBlock, BlockedDeviceData, TypedDevice, PLCData};
pub use monitor::{MonitorList, MonitorRequest, MonitoredDevice};
pub use manager::{SLMPConnectionManager, SLMPWorker};

// Constants
const BUFSIZE: usize = 2048;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_SEND_TIMEOUT_SEC: Duration = Duration::from_secs(1);
const DEFAULT_RECV_TIMEOUT_SEC: Duration = Duration::from_secs(1);

const SUBHEADER_LEN: usize = 15;

/// Result alias for fallible [`SLMPClient`] operations.
pub type SlmpResult<T> = core::result::Result<T, SlmpError>;

/// Structured error type returned by fallible [`SLMPClient`] operations.
#[derive(Debug)]
pub enum SlmpError {
    /// A complete, length-consistent frame arrived but the SLMP end code was
    /// non-zero: the device rejected this request. Bytes are still aligned to
    /// request boundaries, so the caller MAY continue on the same connection
    /// and treat only this request as failed.
    Device { end_code: u16 },
    /// The response structure itself is corrupt (bad length / bad fixed field /
    /// echo mismatch). The byte stream may be desynchronized; the caller SHOULD
    /// drop the connection.
    Framing(FramingError),
    /// A send/receive/connect deadline elapsed.
    Timeout,
    /// The stream is not connected.
    NotConnected,
    /// Any other transport/IO failure (connection refused, reset, broken pipe,
    /// EOF, DNS, address resolution, etc.).
    Io(std::io::Error),
}

/// Describes why a received SLMP response frame is structurally invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FramingError {
    /// Frame shorter than the minimum SLMP 4E response (fixed header + end code).
    ShortFrame { len: usize, min: usize },
    /// The declared data-block length disagrees with the bytes actually received.
    LengthMismatch { declared: usize, actual: usize },
    /// A fixed header field held an unexpected value.
    UnexpectedField { field: &'static str },
    /// An echo response body did not match the payload that was sent.
    EchoMismatch,
}

impl std::fmt::Display for SlmpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlmpError::Device { end_code } => write!(f, "SLMP device error: {} (0x{:04X})", end_code_name(*end_code), end_code),
            SlmpError::Framing(e) => write!(f, "SLMP framing error: {e}"),
            SlmpError::Timeout => write!(f, "SLMP operation timed out"),
            SlmpError::NotConnected => write!(f, "SLMP stream is not connected"),
            SlmpError::Io(e) => write!(f, "SLMP I/O error: {e}"),
        }
    }
}

impl std::error::Error for SlmpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SlmpError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl std::fmt::Display for FramingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FramingError::ShortFrame { len, min } => write!(f, "response frame too short: got {len} bytes, need at least {min}"),
            FramingError::LengthMismatch { declared, actual } => write!(f, "declared data length {declared} does not match actual data length {actual}"),
            FramingError::UnexpectedField { field } => write!(f, "unexpected value in field '{field}'"),
            FramingError::EchoMismatch => write!(f, "echo response body did not match the payload that was sent"),
        }
    }
}

impl std::error::Error for FramingError {}

impl From<std::io::Error> for SlmpError {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::TimedOut => SlmpError::Timeout,
            std::io::ErrorKind::NotConnected => SlmpError::NotConnected,
            _ => SlmpError::Io(e),
        }
    }
}

/// Returns the symbolic name for a non-zero SLMP end code, or `"Unknown Error"`.
pub fn end_code_name(code: u16) -> &'static str {
    match code {
        0xC059 => "WrongCommand",
        0xC05C => "WrongFormat",
        0xC061 => "WrongLength",
        0xCEE0 => "Busy",
        0xCEE1 => "ExceedReqLength",
        0xCEE2 => "ExceedRespLength",
        0xCF10 => "ServerNotFound",
        0xCF20 => "WrongConfigItem",
        0xCF30 => "PrmIDNotFound",
        0xCF31 => "NotStartExclusiveWrite",
        0xCF70 => "RelayFailure",
        0xCF71 => "TimeoutError",
        _ => "Unknown Error",
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "json-api", serde(rename_all = "PascalCase"))]
pub enum CPU {Q, R, L}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "json-api", serde(rename_all = "camelCase"))]
pub struct SLMP4EConnectionProps {
    pub ip: String,
    pub port : u16,
    pub cpu: CPU,
    pub serial_id: u16,
    pub network_id: u8,
    pub pc_id: u8,
    pub io_id: u16,
    pub area_id: u8,
    pub cpu_timer: u16,
}

impl<'a> TryFrom<&'a SLMP4EConnectionProps> for SocketAddr {
    type Error = std::io::Error;
    fn try_from(value: &'a SLMP4EConnectionProps) -> Result<Self, Self::Error> {
        let ip: IpAddr = value.ip.parse::<IpAddr>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let port: u16 = value.port;
        Ok(SocketAddr::new(ip, port))
    }
}

#[inline(always)]
const fn create_subheader(connection_props: &SLMP4EConnectionProps, command_len: usize) -> [u8; SUBHEADER_LEN] {
    const BLANK_CODE: u8 = 0x00;
    const REQUEST_CODE: [u8; 2] = [0x54, 0x00];
    const CPUTIMER_LEN: usize = 2;

    let serial_id: [u8; 2] = connection_props.serial_id.to_le_bytes();
    let io_id: [u8; 2] = connection_props.io_id.to_le_bytes();
    let cpu_timer: [u8; 2] = connection_props.cpu_timer.to_le_bytes();

    // "Command length" counts the packet from cpu_timer
    let command_len: [u8; 2] = ((command_len + CPUTIMER_LEN) as u16).to_le_bytes();

    [
        REQUEST_CODE[0], REQUEST_CODE[1],
        serial_id[0], serial_id[1],
        BLANK_CODE, BLANK_CODE,
        connection_props.network_id,
        connection_props.pc_id,
        io_id[0], io_id[1],
        connection_props.area_id,
        command_len[0], command_len[1],
        cpu_timer[0], cpu_timer[1],
    ]
}

#[derive(Clone)]
pub struct SLMPClient {
    connection_props: SLMP4EConnectionProps,
    stream: Arc<Mutex<Option<TcpStream>>>,
    send_timeout: Duration,
    recv_timeout: Duration,
    buffer: [u8; BUFSIZE],
}

impl SLMPClient {
    pub fn new(connection_props: SLMP4EConnectionProps) -> Self {
        Self {
            connection_props,
            stream: Arc::new(Mutex::new(None)),
            send_timeout: DEFAULT_SEND_TIMEOUT_SEC,
            recv_timeout: DEFAULT_RECV_TIMEOUT_SEC,
            buffer: [0; BUFSIZE],
        }
    }

    pub async fn close(&self) {
        let mut lock = self.stream.lock().await;
        if let Some(mut stream) = lock.take() {
            let _ = stream.shutdown().await;
        }
    }

    #[allow(dead_code)]
    pub fn set_send_timeout(&mut self, dur: Duration) {
        self.send_timeout = dur;
    }

    #[allow(dead_code)]
    pub fn set_recv_timeout(&mut self, dur: Duration) {
        self.recv_timeout = dur;
    }

    pub async fn connect(&self) -> SlmpResult<()> {
        self.close().await;

        let addr: (&str, u16) = (&self.connection_props.ip, self.connection_props.port);
        let socket_addr: SocketAddr = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| SlmpError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, "resolve failed")))?;

        let stream: TcpStream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(socket_addr))
            .await.map_err(|_| SlmpError::Timeout)??;

        let mut lock = self.stream.lock().await;
        *lock = Some(stream);

        Ok(())
    }

    async fn request_response(&mut self, msg: &[u8]) -> SlmpResult<&[u8]> {
        const RECVFRAME_PREFIX_FIXED_LEN: usize = 15;

        let msg_len: usize = msg.len();
        let subheader: [u8; SUBHEADER_LEN] = create_subheader(&self.connection_props, msg_len);

        let mut send_msg: Vec<u8> = Vec::with_capacity(SUBHEADER_LEN + msg_len);
        send_msg.extend(&subheader);
        send_msg.extend(msg);

        let mut stream = self.stream.lock().await;
        let stream = stream.as_mut().ok_or(SlmpError::NotConnected)?;

        timeout(self.send_timeout, stream.write_all(&send_msg)).await
            .map_err(|_| SlmpError::Timeout)??;

        let bytes_read = timeout(self.recv_timeout, stream.read(&mut self.buffer)).await
            .map_err(|_| SlmpError::Timeout)??;

        self.validate_response(&self.buffer[..bytes_read])?;

        Ok(&self.buffer[RECVFRAME_PREFIX_FIXED_LEN..bytes_read])
    }

    fn validate_response(&self, data: &[u8]) -> SlmpResult<()> {
        const FIXED_FRAME_LEN: usize = 13;
        const MIN_FRAME_LEN: usize = 15;
        const RESPONSE_CODE: [u8; 2] = [0xD4, 0x00];
        const BLANK_CODE: u8 = 0x00;

        macro_rules! check_field {
            ($data:expr, $idx:expr, $expected:expr, $field:expr) => {
                if $data[$idx] != $expected {
                    return Err(SlmpError::Framing(FramingError::UnexpectedField { field: $field }));
                }
            };
        }

        let data_len: usize = data.len();
        if data_len < MIN_FRAME_LEN {
            return Err(SlmpError::Framing(FramingError::ShortFrame { len: data_len, min: MIN_FRAME_LEN }));
        }

        let data_block_len: usize = u16::from_le_bytes([data[11], data[12]]) as usize;
        if data_block_len != data_len - FIXED_FRAME_LEN {
            return Err(SlmpError::Framing(FramingError::LengthMismatch { declared: data_block_len, actual: data_len - FIXED_FRAME_LEN }));
        }

        let end_code = u16::from_le_bytes([data[13], data[14]]);
        if end_code != 0 {
            return Err(SlmpError::Device { end_code });
        }

        check_field!(data, 0..2, RESPONSE_CODE, "response_code");
        check_field!(data, 2..4, self.connection_props.serial_id.to_le_bytes(), "serial_id");
        check_field!(data, 4..6, [BLANK_CODE; 2], "blank");
        check_field!(data, 6, self.connection_props.network_id, "network_id");
        check_field!(data, 7, self.connection_props.pc_id, "pc_id");
        check_field!(data, 8..10, self.connection_props.io_id.to_le_bytes(), "io_id");
        check_field!(data, 10, self.connection_props.area_id, "area_id");

        Ok(())
    }

    /* Unit Control */

    pub async fn run_cpu(&mut self) -> SlmpResult<()> {
        const COMMAND: [u8; 8] = unit_control::remote_run();
        self.request_response(&COMMAND).await.map(|_| ())
    }

    pub async fn stop_cpu(&mut self) -> SlmpResult<()> {
        const COMMAND: [u8; 6] = unit_control::remote_stop();
        self.request_response(&COMMAND).await.map(|_| ())
    }

    pub async fn pause_cpu(&mut self) -> SlmpResult<()> {
        const COMMAND: [u8; 6] = unit_control::remote_pause();
        self.request_response(&COMMAND).await.map(|_| ())
    }

    pub async fn clear_latch(&mut self) -> SlmpResult<()> {
        const COMMAND: [u8; 6] = unit_control::remote_latch_clear();
        self.request_response(&COMMAND).await.map(|_| ())
    }

    pub async fn reset_cpu(&mut self) -> SlmpResult<()> {
        const COMMAND: [u8; 6] = unit_control::remote_reset();
        self.request_response(&COMMAND).await.map(|_| ())
    }

    pub async fn get_cpu_type(&mut self) -> SlmpResult<String> {
        const COMMAND: [u8; 4] = unit_control::get_cpu_type();
        let ret = self.request_response(&COMMAND).await?;

        const END_CODE: u8 = 0x20;
        let end_pos = ret.iter().position(|&b| b == END_CODE).unwrap_or(ret.len());
        let cpu_type = String::from_utf8_lossy(&ret[..end_pos]).into_owned();

        Ok(cpu_type)
    }

    pub async fn lock_cpu(&mut self, password: &str) -> SlmpResult<()> {
        let cmd = unit_control::lock_cpu(&self.connection_props.cpu, password)?;
        self.request_response(&cmd).await.map(|_| ())
    }

    pub async fn unlock_cpu(&mut self, password: &str) -> SlmpResult<()> {
        let cmd = unit_control::unlock_cpu(&self.connection_props.cpu, password)?;
        self.request_response(&cmd).await.map(|_| ())
    }

    pub async fn echo(&mut self) -> SlmpResult<()> {
        const COMMAND: [u8; 10] = unit_control::echo();
        let recv = self.request_response(&COMMAND).await
            .map_err(|_| SlmpError::Timeout)?;

        if &recv[2..6] ==  unit_control::ECHO_MESSAGE {
            Ok(())
        } else {
            return Err(SlmpError::Framing(FramingError::EchoMismatch))
        }
    }

    /* File Control */

    /* Device Access */

    pub async fn bulk_write<'a>(&mut self, start_device: Device, data: &'a [TypedData]) -> SlmpResult<()>
    {
        if data.len() > 0 {
            let query = SLMPBulkWriteQuery {
                cpu: &self.connection_props.cpu,
                start_device,
                data,
            };
            let cmd: SLMPBulkWriteCommand = query.into();

            self.request_response(&cmd).await.map(|_| ())?;
        }

        Ok(())
    }


    pub async fn random_write<'a>(&mut self, data: &'a [DeviceData]) -> SlmpResult<()>
    {
        // Word access
        let mut sorted_word_data: Vec<DeviceData> = data.iter()
            .filter(|x| !matches!(x.data, TypedData::Bool(_)))
            .copied()
            .collect();
        sorted_word_data.sort_by_key(|p| p.device.address);
        sorted_word_data.sort_by_key(|p| p.data.get_type());

        // Bit access
        let mut sorted_bit_data: Vec<DeviceData> = data.iter()
            .filter(|x| matches!(x.data, TypedData::Bool(_)))
            .copied()
            .collect();
        sorted_bit_data.sort_by_key(|p| p.device.address);

        let single_word_access_points_for_multi_word_communication = sorted_word_data
            .iter()
            .filter(|x| matches!(x.data.get_type().device_size(), DeviceSize::MultiWord(_)))
            .fold(0, |a, b| {
                if let DeviceSize::MultiWord(n) = b.data.get_type().device_size() { a + n } else { a }
            });

        let single_word_access_points: u8 = sorted_word_data
            .iter()
            .filter(|x| x.data.get_type().device_size() == DeviceSize::SingleWord)
            .count() as u8 + single_word_access_points_for_multi_word_communication;

        let double_word_access_points: u8 = sorted_word_data
            .iter()
            .filter(|x| x.data.get_type().device_size() == DeviceSize::DoubleWord)
            .count() as u8;

        let bit_access_points: u8 = sorted_bit_data
            .iter()
            .filter(|x| x.data.get_type().device_size() == DeviceSize::Bit).count() as u8;

        if single_word_access_points + double_word_access_points > 0 {
            let query = SLMPRandomWriteQuery {
                cpu: &self.connection_props.cpu,
                sorted_data: &sorted_word_data,
                access_type: AccessType::Word,
                bit_access_points: 0,
                single_word_access_points,
                double_word_access_points,
            };
            let cmd: SLMPRandomWriteCommand = query.into();

            self.request_response(&cmd).await.map(|_| ())?;
        }

        if bit_access_points > 0 {
            let query = SLMPRandomWriteQuery {
                cpu: &self.connection_props.cpu,
                sorted_data: &sorted_bit_data,
                access_type: AccessType::Bit,
                bit_access_points,
                single_word_access_points: 0,
                double_word_access_points: 0,
            };
            let cmd: SLMPRandomWriteCommand = query.into();

            self.request_response(&cmd).await.map(|_| ())?;
        }

        Ok(())
    }

    pub async fn block_write<'a>(&mut self, data: &'a [BlockedDeviceData<'a>]) -> SlmpResult<()>
    {
        let mut sorted_data = data.to_vec();
        sorted_data.sort_by_key(|p| p.access_type);

        let word_access_points: u8 = sorted_data.iter().filter(|x| x.access_type == AccessType::Word).count() as u8;
        let bit_access_points: u8 = sorted_data.iter().filter(|x| x.access_type == AccessType::Bit).count() as u8;

        if word_access_points + bit_access_points > 0 {
            let query = SLMPBlockWriteQuery {
                cpu: &self.connection_props.cpu,
                sorted_data: &sorted_data,
                word_access_points,
                bit_access_points
            };
            let cmd: SLMPBlockWriteCommand = query.into();

            self.request_response(&cmd).await.map(|_| ())?;
        }

        Ok(())
    }

    pub async fn bulk_read(&mut self, start_device: Device, device_num: usize, data_type: DataType) -> SlmpResult<Vec<DeviceData>>
    {
        let query = SLMPBulkReadQuery {
            cpu: &self.connection_props.cpu,
            start_device,
            device_num,
            data_type,
        };
        let cmd: SLMPBulkReadCommand = query.into();

        let recv: &[u8] = &(self.request_response(&cmd).await?);

        match data_type {
            DataType::Bool => {
                let device_type = start_device.device_type;
                let start_address = start_device.address;

                let mut ret: Vec<DeviceData> = Vec::with_capacity(device_num);
                for (i, data) in recv.iter().flat_map(|&x| [(x >> 4) & 0x01, x & 0x01]).enumerate() {
                    if i < device_num {
                        ret.push(DeviceData {
                            device: Device {device_type, address: start_address + i},
                            data: TypedData::Bool(if data == 1 { true } else { false })
                        })
                    }
                }
                Ok(ret)
            }
            _ => {
                let chunk_size = data_type.byte_size();
                let skip_address = chunk_size / 2;
                let device_type = start_device.device_type;
                let start_address = start_device.address;

                let mut ret: Vec<DeviceData> = Vec::with_capacity(device_num);
                for (i, data) in recv.chunks_exact(chunk_size).enumerate() {
                    ret.push(DeviceData {
                        device: Device {device_type, address: start_address + skip_address * i},
                        data: TypedData::from((data, data_type))
                    });
                }

                Ok(ret)
            }
        }
    }

    pub async fn random_read(&mut self, devices: &[TypedDevice]) -> SlmpResult<Vec<DeviceData>>
    {
        let monitor_list = MonitorList::from(devices);

        let query = SLMPRandomReadQuery {
            cpu: &self.connection_props.cpu,
            monitor_list: &monitor_list
        };
        let cmd: SLMPRandomReadCommand = query.into();

        let recv: &[u8] = &(self.request_response(&cmd).await?);

        Ok(monitor_list.parse(&recv))
    }


    pub async fn block_read(&mut self, device_blocks: &[DeviceBlock]) -> SlmpResult<Vec<DeviceData>>
    {
        const WORD_RESPONSE_BYTEELEN: usize = 2;
        const BIT_RESPONSE_BYTEELEN: usize = 1;

        let mut sorted_block = device_blocks.to_vec();
        sorted_block.sort_by_key(|p| p.start_device.address);
        sorted_block.sort_by_key(|p| p.access_type);

        let word_access_points: u8 = sorted_block.iter().filter(|x| x.access_type == AccessType::Word).count() as u8;
        let bit_access_points: u8 = sorted_block.iter().filter(|x| x.access_type == AccessType::Bit).count() as u8;

        let query = SLMPBlockReadQuery {
            cpu: &self.connection_props.cpu,
            sorted_block: &sorted_block,
            word_access_points,
            bit_access_points,
        };
        let cmd: SLMPBlockReadCommand = query.into();

        let recv: &[u8] = &(self.request_response(&cmd).await?);

        let data_num = sorted_block.iter().fold(0, |a, b| a + b.size);
        let mut ret: Vec<DeviceData> = Vec::with_capacity(data_num);

        let mut read_addr = 0;

        for block in &sorted_block {
            let start_address = block.start_device.address;
            let device_type = block.start_device.device_type;
            let block_bytelen = match block.access_type {
                AccessType::Word => WORD_RESPONSE_BYTEELEN * block.size,
                AccessType::Bit => BIT_RESPONSE_BYTEELEN * div_ceil(block.size, 8)
            };
            let blocked_data = &recv[read_addr..(read_addr + block_bytelen)];
            read_addr += block_bytelen;

            match block.access_type {
                AccessType::Word => {
                    for (i, x) in blocked_data.chunks_exact(WORD_RESPONSE_BYTEELEN).enumerate() {
                        ret.push(DeviceData{
                            device: Device {device_type, address: start_address + i},
                            data: TypedData::from((x, DataType::U16)),
                        });
                    }
                },
                AccessType::Bit => {
                    for (i, x) in blocked_data.chunks_exact(BIT_RESPONSE_BYTEELEN).enumerate() {
                        for (j, y) in u8_to_bits(x[0]).into_iter().enumerate() {
                            let bit_index = 8 * i + j;
                            if bit_index < block.size {
                                ret.push(DeviceData{
                                    device: Device {device_type, address: start_address + bit_index},
                                    data: TypedData::Bool(y),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(ret)
    }

    pub async fn monitor_register(&mut self, devices: &[TypedDevice]) -> SlmpResult<MonitorList>
    {
        let monitor_list = MonitorList::from(devices);
        let query = SLMPMonitorRegisterQuery {
            cpu: &self.connection_props.cpu,
            monitor_list: &monitor_list
        };
        let cmd: SLMPMonitorRegisterCommand = query.into();
        self.request_response(&cmd).await?;

        Ok(monitor_list)
    }

    pub async fn monitor_read(&mut self, monitor_list: &MonitorList) -> SlmpResult<Vec<DeviceData>>
    {
        const COMMAND: SLMPMonitorReadCommand = SLMPMonitorReadCommand::new();
        let recv: &[u8] = &(self.request_response(&COMMAND).await?);

        Ok(monitor_list.parse(&recv))
    }

}


#[inline(always)]
pub(crate) const fn div_ceil(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}

#[inline(always)]
pub(crate) const fn u8_to_bits(n: u8) -> [bool; 8] {
    [ n & 0x01 != 0, n & 0x02 != 0, n & 0x04 != 0, n & 0x08 != 0, n & 0x10 != 0, n & 0x20 != 0, n & 0x40 != 0, n & 0x80 != 0, ]
}

#[inline(always)]
pub(crate) const fn bits_to_u8(bits: [bool; 8]) -> u8 {
    ((bits[0] as u8) << 0) |
    ((bits[1] as u8) << 1) |
    ((bits[2] as u8) << 2) |
    ((bits[3] as u8) << 3) |
    ((bits[4] as u8) << 4) |
    ((bits[5] as u8) << 5) |
    ((bits[6] as u8) << 6) |
    ((bits[7] as u8) << 7)
}

#[inline(always)]
pub(crate) const fn u16_to_bits(n: u16) -> [bool; 16] {
    let bytes: [u8; 2] = n.to_le_bytes();

    let low_bits = u8_to_bits(bytes[0]);
    let high_bits = u8_to_bits(bytes[1]);

    [
        low_bits[0], low_bits[1], low_bits[2], low_bits[3], low_bits[4], low_bits[5], low_bits[6], low_bits[7],
        high_bits[0], high_bits[1], high_bits[2], high_bits[3], high_bits[4], high_bits[5], high_bits[6], high_bits[7],
    ]
}

#[inline(always)]
pub(crate) const fn bits_to_u16(bits: [bool; 16]) -> u16 {
    let low_bits = [bits[0], bits[1], bits[2], bits[3], bits[4], bits[5], bits[6], bits[7]];
    let high_bits = [bits[8], bits[9], bits[10], bits[11], bits[12], bits[13], bits[14], bits[15]];

    let high_byte = bits_to_u8(high_bits);
    let low_byte = bits_to_u8(low_bits);

    u16::from_be_bytes([low_byte, high_byte])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_props() -> SLMP4EConnectionProps {
        SLMP4EConnectionProps {
            ip: "127.0.0.1".to_string(),
            port: 0,
            cpu: CPU::R,
            serial_id: 0,
            network_id: 0,
            pc_id: 0xFF,
            io_id: 0x03FF,
            area_id: 0,
            cpu_timer: 0,
        }
    }

    /// Builds a well-formed SLMP 4E response frame whose fixed fields match
    /// `props`, with the given end code and payload, and a correctly computed
    /// data-block-length field.
    fn build_frame(props: &SLMP4EConnectionProps, end_code: u16, payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&[0xD4, 0x00]); // response_code
        frame.extend_from_slice(&props.serial_id.to_le_bytes());
        frame.extend_from_slice(&[0x00, 0x00]); // blank
        frame.push(props.network_id);
        frame.push(props.pc_id);
        frame.extend_from_slice(&props.io_id.to_le_bytes());
        frame.push(props.area_id);

        let data_block_len: u16 = (2 + payload.len()) as u16;
        frame.extend_from_slice(&data_block_len.to_le_bytes());

        frame.extend_from_slice(&end_code.to_le_bytes());
        frame.extend_from_slice(payload);

        frame
    }

    #[test]
    fn end_code_nonzero_is_device_not_framing() {
        let props = dummy_props();
        let client = SLMPClient::new(props.clone());
        let frame = build_frame(&props, 0xC059, &[]);

        let result = client.validate_response(&frame);

        match result {
            Err(SlmpError::Device { end_code }) => assert_eq!(end_code, 0xC059),
            other => panic!("expected Err(SlmpError::Device {{ .. }}), got {other:?}"),
        }
    }

    #[test]
    fn malformed_frame_is_framing() {
        let props = dummy_props();
        let client = SLMPClient::new(props.clone());
        let mut frame = build_frame(&props, 0, &[]);

        // Declare a data-block length of 10 while only 2 bytes (the end code)
        // actually follow the fixed header.
        frame[11..13].copy_from_slice(&10u16.to_le_bytes());

        let result = client.validate_response(&frame);

        match result {
            Err(SlmpError::Framing(FramingError::LengthMismatch { declared, actual })) => {
                assert_eq!(declared, 10);
                assert_eq!(actual, 2);
            }
            other => panic!("expected Err(SlmpError::Framing(FramingError::LengthMismatch {{ .. }})), got {other:?}"),
        }
    }

    #[test]
    fn short_frame_is_framing_not_panic() {
        let props = dummy_props();
        let client = SLMPClient::new(props);
        let frame = vec![0u8; 13];

        let result = client.validate_response(&frame);

        match result {
            Err(SlmpError::Framing(FramingError::ShortFrame { len, min })) => {
                assert_eq!(len, 13);
                assert_eq!(min, 15);
            }
            other => panic!("expected Err(SlmpError::Framing(FramingError::ShortFrame {{ .. }})), got {other:?}"),
        }
    }

    #[test]
    fn zero_end_code_ok() {
        let props = dummy_props();
        let client = SLMPClient::new(props.clone());
        let frame = build_frame(&props, 0, &[]);

        let result = client.validate_response(&frame);

        assert!(matches!(result, Ok(())));
    }

    #[test]
    fn wrong_fixed_field_is_framing() {
        let props = dummy_props();
        let client = SLMPClient::new(props.clone());
        let mut frame = build_frame(&props, 0, &[]);

        // Corrupt the response code field.
        frame[0] = 0x00;
        frame[1] = 0x00;

        let result = client.validate_response(&frame);

        match result {
            Err(SlmpError::Framing(FramingError::UnexpectedField { field })) => {
                assert_eq!(field, "response_code");
            }
            other => panic!("expected Err(SlmpError::Framing(FramingError::UnexpectedField {{ .. }})), got {other:?}"),
        }
    }

    #[test]
    fn io_error_maps_to_variants() {
        assert!(matches!(
            SlmpError::from(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            SlmpError::Timeout
        ));
        assert!(matches!(
            SlmpError::from(std::io::Error::from(std::io::ErrorKind::NotConnected)),
            SlmpError::NotConnected
        ));
        assert!(matches!(
            SlmpError::from(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            SlmpError::Io(_)
        ));
    }
}
