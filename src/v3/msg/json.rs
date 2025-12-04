//! JSON message definitions.

use std::{borrow::Cow, ops::Deref};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_lite::{Deserialize, Intermediate, Serialize};

use crate::v3::{
    error::Error,
    msg::{DecodeMessage, EncodeMessage, EncodedMessage, MessageKind},
};

/// JSON-RPC value.
pub type JsonRpcValue = serde_lite::Intermediate;

/// JSON-RPC object.
pub type JsonRpcObject = serde_lite::Map;

/// JSON-RPC request.
#[derive(Deserialize, Serialize)]
pub struct JsonRpcRequest {
    jsonrpc: JsonRpcVersion,
    id: u64,
    method: JsonRpcMethod,
    params: JsonRpcParams,
}

impl JsonRpcRequest {
    /// Create a new JSON-RPC request.
    #[inline]
    pub const fn new(id: u64, method: JsonRpcMethod, params: JsonRpcParams) -> Self {
        Self {
            jsonrpc: JsonRpcVersion,
            id,
            method,
            params,
        }
    }

    /// Get the request ID.
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Split the request into its method and parameters.
    #[inline]
    pub fn deconstruct(self) -> (JsonRpcMethod, JsonRpcParams) {
        (self.method, self.params)
    }
}

impl DecodeMessage for JsonRpcRequest {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::JsonRpcRequest);

        let data = encoded.data();

        decode_json_value(&mut data.clone())
            .map_err(|err| Error::from_static_msg_and_cause("invalid JSON-RPC request", err))
    }
}

impl EncodeMessage for JsonRpcRequest {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let data = encode_json_value(self, buf).expect("unable to serialize JSON-RPC request");

        EncodedMessage::new(MessageKind::JsonRpcRequest, data)
    }
}

/// JSON-RPC response.
pub enum JsonRpcResponse {
    Success(JsonRpcSuccessResponse),
    Error(JsonRpcErrorResponse),
}

impl JsonRpcResponse {
    /// Create a new success response.
    #[inline]
    pub const fn success(id: u64, result: JsonRpcValue) -> Self {
        Self::Success(JsonRpcSuccessResponse::new(id, result))
    }

    /// Create a new error response.
    #[inline]
    pub const fn error(id: u64, error: JsonRpcError) -> Self {
        Self::Error(JsonRpcErrorResponse::new(id, error))
    }

    /// Get ID of the corresponding request.
    pub fn id(&self) -> u64 {
        match self {
            Self::Success(r) => r.id,
            Self::Error(r) => r.id,
        }
    }
}

impl DecodeMessage for JsonRpcResponse {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::JsonRpcResponse);

        let data = encoded.data();

        let r: JsonRpcUnifiedResponse = decode_json_value(&mut data.clone())
            .map_err(|err| Error::from_static_msg_and_cause("invalid JSON-RPC response", err))?;

        let res = match (r.result, r.error) {
            (OptionalJsonField::Present(res), OptionalJsonField::Absent) => {
                Self::Success(JsonRpcSuccessResponse::new(r.id, res))
            }
            (OptionalJsonField::Absent, OptionalJsonField::Present(err)) => {
                Self::Error(JsonRpcErrorResponse::new(r.id, err))
            }
            _ => {
                return Err(Error::from_static_msg(
                    "invalid JSON-RPC response: either result or error field must be present",
                ));
            }
        };

        Ok(res)
    }
}

impl EncodeMessage for JsonRpcResponse {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        match self {
            Self::Success(r) => r.encode(buf),
            Self::Error(r) => r.encode(buf),
        }
    }
}

/// JSON-RPC success response.
#[derive(Deserialize, Serialize)]
pub struct JsonRpcSuccessResponse {
    jsonrpc: JsonRpcVersion,
    id: u64,
    result: JsonRpcValue,
}

impl JsonRpcSuccessResponse {
    /// Create a new JSON-RPC success response.
    #[inline]
    const fn new(id: u64, result: JsonRpcValue) -> Self {
        Self {
            jsonrpc: JsonRpcVersion,
            id,
            result,
        }
    }

    /// Encode the response.
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let data =
            encode_json_value(self, buf).expect("unable to serialize JSON-RPC success response");

        EncodedMessage::new(MessageKind::JsonRpcResponse, data)
    }

    /// Deserialize the result.
    pub fn decode_result<T>(self) -> Result<T, Error>
    where
        T: Deserialize,
    {
        T::deserialize(&self.result).map_err(|err| {
            Error::from_static_msg_and_cause("unable to deserialize JSON-RPC result", err)
        })
    }
}

/// JSON-RPC error response.
#[derive(Deserialize, Serialize)]
pub struct JsonRpcErrorResponse {
    jsonrpc: JsonRpcVersion,
    id: u64,
    error: JsonRpcError,
}

impl JsonRpcErrorResponse {
    /// Create a new JSON-RPC error response.
    #[inline]
    const fn new(id: u64, error: JsonRpcError) -> Self {
        Self {
            jsonrpc: JsonRpcVersion,
            id,
            error,
        }
    }

    /// Encode the response.
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let data =
            encode_json_value(self, buf).expect("unable to serialize JSON-RPC error response");

        EncodedMessage::new(MessageKind::JsonRpcResponse, data)
    }

    /// Get the error.
    #[inline]
    pub fn error(&self) -> &JsonRpcError {
        &self.error
    }
}

/// JSON-RPC error.
#[derive(Deserialize, Serialize)]
pub struct JsonRpcError {
    code: i64,
    message: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<JsonRpcValue>,
}

impl JsonRpcError {
    /// Create a new JSON-RPC error.
    pub fn new<T>(code: i64, message: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Set the error message.
    pub fn with_message<T>(mut self, message: T) -> Self
    where
        T: Into<String>,
    {
        self.message = message.into();
        self
    }

    /// Set the error data.
    #[inline]
    pub fn with_data(mut self, data: Option<JsonRpcValue>) -> Self {
        self.data = data;
        self
    }

    /// Get the error code.
    #[inline]
    pub fn code(&self) -> i64 {
        self.code
    }

    /// Get the error message.
    #[inline]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Split the error into its components.
    #[inline]
    pub fn deconstruct(self) -> (i64, String, Option<JsonRpcValue>) {
        (self.code, self.message, self.data)
    }
}

impl From<JsonRpcErrorResponse> for JsonRpcError {
    #[inline]
    fn from(resp: JsonRpcErrorResponse) -> Self {
        resp.error
    }
}

/// Unified JSON-RPC response type used for deserialization.
#[derive(Deserialize)]
struct JsonRpcUnifiedResponse {
    #[allow(unused)]
    jsonrpc: JsonRpcVersion,

    id: u64,

    #[serde(default)]
    result: OptionalJsonField<JsonRpcValue>,

    #[serde(default)]
    error: OptionalJsonField<JsonRpcError>,
}

/// JSON-RPC notification.
#[derive(Deserialize, Serialize)]
pub struct JsonRpcNotification {
    jsonrpc: JsonRpcVersion,
    method: JsonRpcMethod,
    params: JsonRpcParams,
}

impl JsonRpcNotification {
    /// Create a new JSON-RPC notification.
    #[inline]
    pub const fn new(method: JsonRpcMethod, params: JsonRpcParams) -> Self {
        Self {
            jsonrpc: JsonRpcVersion,
            method,
            params,
        }
    }

    /// Split the notification into its method and parameters.
    #[inline]
    pub fn deconstruct(self) -> (JsonRpcMethod, JsonRpcParams) {
        (self.method, self.params)
    }
}

impl DecodeMessage for JsonRpcNotification {
    fn decode(encoded: &EncodedMessage) -> Result<Self, Error> {
        assert_eq!(encoded.kind(), MessageKind::JsonRpcNotification);

        let data = encoded.data();

        decode_json_value(&mut data.clone())
            .map_err(|err| Error::from_static_msg_and_cause("invalid JSON-RPC notification", err))
    }
}

impl EncodeMessage for JsonRpcNotification {
    fn encode(&self, buf: &mut BytesMut) -> EncodedMessage {
        let data = encode_json_value(self, buf).expect("unable to serialize JSON-RPC notification");

        EncodedMessage::new(MessageKind::JsonRpcNotification, data)
    }
}

/// Helper type.
struct JsonRpcVersion;

impl Deserialize for JsonRpcVersion {
    fn deserialize(val: &Intermediate) -> Result<Self, serde_lite::Error> {
        let version: String = Deserialize::deserialize(val)?;

        if version == "2.0" {
            Ok(JsonRpcVersion)
        } else {
            Err(serde_lite::Error::custom("unsupported JSON-RPC version"))
        }
    }
}

impl Serialize for JsonRpcVersion {
    fn serialize(&self) -> Result<Intermediate, serde_lite::Error> {
        Serialize::serialize(&"2.0")
    }
}

/// JSON-RPC method.
pub struct JsonRpcMethod {
    inner: Cow<'static, str>,
}

impl Deref for JsonRpcMethod {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<&'static str> for JsonRpcMethod {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self {
            inner: Cow::Borrowed(s),
        }
    }
}

impl From<String> for JsonRpcMethod {
    #[inline]
    fn from(s: String) -> Self {
        Self {
            inner: Cow::Owned(s),
        }
    }
}

impl Deserialize for JsonRpcMethod {
    fn deserialize(val: &Intermediate) -> Result<Self, serde_lite::Error> {
        let s = String::deserialize(val)?;

        let res = Self {
            inner: Cow::Owned(s),
        };

        Ok(res)
    }
}

impl Serialize for JsonRpcMethod {
    #[inline]
    fn serialize(&self) -> Result<Intermediate, serde_lite::Error> {
        <&str>::serialize(&self.inner.as_ref())
    }
}

/// JSON-RPC parameters.
pub struct JsonRpcParams {
    inner: JsonRpcValue,
}

impl JsonRpcParams {
    /// Create new JSON-RPC parameters from a given serializable value.
    pub fn new<T>(value: T) -> Result<Self, Error>
    where
        T: Serialize,
    {
        let val = value
            .serialize()
            .map_err(|err| Error::from_static_msg_and_cause("invalid JSON-RPC parameters", err))?;

        let obj = match val {
            Intermediate::Map(obj) => obj,
            Intermediate::None => JsonRpcObject::new(),
            _ => {
                return Err(Error::from_static_msg(
                    "invalid JSON-RPC parameters: expected object",
                ));
            }
        };

        let res = Self {
            inner: Intermediate::Map(obj),
        };

        Ok(res)
    }

    /// Deserialize the parameters.
    pub fn decode<T>(&self) -> Result<T, Error>
    where
        T: Deserialize,
    {
        T::deserialize(&self.inner)
            .map_err(|err| Error::from_static_msg_and_cause("invalid JSON-RPC parameters", err))
    }
}

impl Deserialize for JsonRpcParams {
    fn deserialize(val: &Intermediate) -> Result<Self, serde_lite::Error> {
        let Some(obj) = val.as_map() else {
            return Err(serde_lite::Error::custom(
                "invalid JSON-RPC parameters: expected object",
            ));
        };

        let res = Self {
            inner: Intermediate::Map(obj.clone()),
        };

        Ok(res)
    }
}

impl Serialize for JsonRpcParams {
    #[inline]
    fn serialize(&self) -> Result<Intermediate, serde_lite::Error> {
        Ok(self.inner.clone())
    }
}

/// Helper type.
#[derive(Default)]
enum OptionalJsonField<T> {
    #[default]
    Absent,
    Present(T),
}

impl<T> Deserialize for OptionalJsonField<T>
where
    T: Deserialize,
{
    fn deserialize(val: &Intermediate) -> Result<Self, serde_lite::Error> {
        let val = T::deserialize(val)?;

        let res = Self::Present(val);

        Ok(res)
    }
}

/// Decode a JSON value from a given buffer.
fn decode_json_value<T>(buf: &mut Bytes) -> Result<T, Error>
where
    T: Deserialize,
{
    // helper function to avoid excessive monomorphization
    fn inner(buf: &mut Bytes) -> Result<Intermediate, Error> {
        serde_json::from_reader(buf.reader()).map_err(Error::from_other)
    }

    inner(buf)
        .map(|v| T::deserialize(&v))
        .and_then(|res| res.map_err(Error::from_other))
}

/// Encode a JSON value.
fn encode_json_value<T>(value: &T, buf: &mut BytesMut) -> Result<Bytes, Error>
where
    T: Serialize,
{
    // helper function to avoid excessive monomorphization
    fn inner(value: &Intermediate, buf: &mut BytesMut) -> Result<Bytes, Error> {
        let writer = buf.writer();

        serde_json::to_writer(writer, value).map_err(Error::from_other)?;

        let data = buf.split();

        Ok(data.freeze())
    }

    value
        .serialize()
        .map_err(Error::from_other)
        .and_then(|v| inner(&v, buf))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use serde_lite::{Deserialize, Serialize};

    use crate::v3::msg::{DecodeMessage, EncodeMessage, EncodedMessage, MessageKind};

    use super::{
        JsonRpcError, JsonRpcMethod, JsonRpcNotification, JsonRpcParams, JsonRpcRequest,
        JsonRpcResponse,
    };

    #[test]
    fn test_request_serialization() {
        // helper struct
        #[derive(Serialize)]
        struct CustomParams {
            foo: String,
            bar: u32,
        }

        let custom_params = CustomParams {
            foo: String::from("hello"),
            bar: 42,
        };

        let method = JsonRpcMethod::from("custom_method");
        let params = JsonRpcParams::new(custom_params);

        let req = JsonRpcRequest::new(1, method, params.unwrap());

        let mut buf = BytesMut::new();

        let encoded = req.encode(&mut buf);

        let expected_json = r#"{"jsonrpc":"2.0","id":1,"method":"custom_method","params":{"foo":"hello","bar":42}}"#;

        assert_eq!(encoded.data(), expected_json.as_bytes());
    }

    #[test]
    fn test_request_deserialization() {
        // helper struct
        #[derive(Deserialize)]
        struct CustomParams {
            foo: String,
            bar: u32,
        }

        let buf = Bytes::from(
            r#"{"jsonrpc":"2.0","id":1,"method":"custom_method","params":{"foo":"hello","bar":42}}"#,
        );

        let encoded = EncodedMessage::new(MessageKind::JsonRpcRequest, buf);

        let decoded = JsonRpcRequest::decode(&encoded).unwrap();

        assert_eq!(decoded.id(), 1);

        let (method, params) = decoded.deconstruct();

        assert_eq!(&*method, "custom_method");

        let custom_params = params.decode::<CustomParams>().unwrap();

        assert_eq!(custom_params.foo, "hello");
        assert_eq!(custom_params.bar, 42);
    }

    #[test]
    fn test_notification_serialization() {
        // helper struct
        #[derive(Serialize)]
        struct CustomParams {
            foo: String,
            bar: u32,
        }

        let custom_params = CustomParams {
            foo: String::from("hello"),
            bar: 42,
        };

        let method = JsonRpcMethod::from("custom_method");
        let params = JsonRpcParams::new(custom_params);

        let req = JsonRpcNotification::new(method, params.unwrap());

        let mut buf = BytesMut::new();

        let encoded = req.encode(&mut buf);

        let expected_json =
            r#"{"jsonrpc":"2.0","method":"custom_method","params":{"foo":"hello","bar":42}}"#;

        assert_eq!(encoded.data(), expected_json.as_bytes());
    }

    #[test]
    fn test_notification_deserialization() {
        // helper struct
        #[derive(Deserialize)]
        struct CustomParams {
            foo: String,
            bar: u32,
        }

        let buf = Bytes::from(
            r#"{"jsonrpc":"2.0","method":"custom_method","params":{"foo":"hello","bar":42}}"#,
        );

        let encoded = EncodedMessage::new(MessageKind::JsonRpcNotification, buf);

        let decoded = JsonRpcNotification::decode(&encoded).unwrap();

        let (method, params) = decoded.deconstruct();

        assert_eq!(&*method, "custom_method");

        let custom_params = params.decode::<CustomParams>().unwrap();

        assert_eq!(custom_params.foo, "hello");
        assert_eq!(custom_params.bar, 42);
    }

    #[test]
    fn test_response_serialization() {
        // helper struct
        #[derive(Serialize)]
        struct CustomData {
            foo: String,
            bar: u32,
        }

        let custom_data = CustomData {
            foo: String::from("hello"),
            bar: 42,
        };

        let result = custom_data.serialize().unwrap();

        let response = JsonRpcResponse::success(1, result);

        let mut buf = BytesMut::new();

        let encoded = response.encode(&mut buf);

        let expected_json = r#"{"jsonrpc":"2.0","id":1,"result":{"foo":"hello","bar":42}}"#;

        assert_eq!(encoded.data(), expected_json.as_bytes());
    }

    #[test]
    fn test_error_response_serialization() {
        // helper struct
        #[derive(Serialize)]
        struct CustomData {
            foo: String,
            bar: u32,
        }

        let custom_data = CustomData {
            foo: String::from("hello"),
            bar: 42,
        };

        let error = JsonRpcError::new(123, "")
            .with_message("An error occurred")
            .with_data(None);

        let response = JsonRpcResponse::error(1, error);

        let mut buf = BytesMut::new();

        let encoded = response.encode(&mut buf);

        let expected_json =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":123,"message":"An error occurred"}}"#;

        assert_eq!(encoded.data(), expected_json.as_bytes());

        let data = custom_data.serialize().unwrap();

        let error = JsonRpcError::new(123, "")
            .with_message("An error occurred")
            .with_data(Some(data));

        let response = JsonRpcResponse::error(1, error);

        let encoded = response.encode(&mut buf);

        let expected_json = r#"{"jsonrpc":"2.0","id":1,"error":{"code":123,"message":"An error occurred","data":{"foo":"hello","bar":42}}}"#;

        assert_eq!(encoded.data(), expected_json.as_bytes());
    }

    #[test]
    fn test_response_deserialization() {
        // helper struct
        #[derive(Deserialize)]
        struct CustomData {
            foo: String,
            bar: u32,
        }

        let buf = Bytes::from(r#"{"jsonrpc":"2.0","id":1,"result":{"foo":"hello","bar":42}}"#);

        let encoded = EncodedMessage::new(MessageKind::JsonRpcResponse, buf);

        let decoded = JsonRpcResponse::decode(&encoded).unwrap();

        match decoded {
            JsonRpcResponse::Success(resp) => {
                assert_eq!(resp.id, 1);

                let data = resp.decode_result::<CustomData>().unwrap();

                assert_eq!(data.foo, "hello");
                assert_eq!(data.bar, 42);
            }
            _ => panic!("expected success response"),
        }
    }

    #[test]
    fn test_error_response_deserialization() {
        // helper struct
        #[derive(Deserialize)]
        struct CustomData {
            foo: String,
            bar: u32,
        }

        let buf = Bytes::from(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":123,"message":"An error occurred"}}"#,
        );

        let encoded = EncodedMessage::new(MessageKind::JsonRpcResponse, buf);

        let decoded = JsonRpcResponse::decode(&encoded).unwrap();

        match decoded {
            JsonRpcResponse::Error(resp) => {
                assert_eq!(resp.id, 1);

                let (code, message, data) = resp.error.deconstruct();

                assert_eq!(code, 123);
                assert_eq!(message, "An error occurred");
                assert!(data.is_none());
            }
            _ => panic!("expected error response"),
        }

        let buf = Bytes::from(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":123,"message":"An error occurred","data":{"foo":"hello","bar":42}}}"#,
        );

        let encoded = EncodedMessage::new(MessageKind::JsonRpcResponse, buf);

        let decoded = JsonRpcResponse::decode(&encoded).unwrap();

        match decoded {
            JsonRpcResponse::Error(resp) => {
                assert_eq!(resp.id, 1);

                let (code, message, data) = resp.error.deconstruct();

                assert_eq!(code, 123);
                assert_eq!(message, "An error occurred");
                assert!(data.is_some());

                let data = data.unwrap();

                let custom_data = CustomData::deserialize(&data).unwrap();

                assert_eq!(custom_data.foo, "hello");
                assert_eq!(custom_data.bar, 42);
            }
            _ => panic!("expected error response"),
        }
    }
}
