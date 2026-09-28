//! Stream sources other than shipstern. Each implements
//! [`solana_sentinel::StreamSource`] and feeds the framework dispatcher, so any
//! transport can drive the same rules: plain Yellowstone gRPC here, Solana
//! PubSub websockets here, anything else by implementing the trait.

pub mod convert;
pub mod grpc;
pub mod websocket;

pub use grpc::GrpcSource;
pub use websocket::WebsocketSource;
