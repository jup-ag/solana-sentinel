//! Where on-chain updates come from. Every source implements shipstern's
//! [`SourceTrait`](shipstern::sources::SourceTrait) and runs inside its
//! runtime, which dispatches each update to the pipelines whose filter name it
//! carries. Yellowstone gRPC tags updates server-side; the websocket and Kafka
//! sources here match locally against the same filter set, so all three drive
//! the same rules. [`MultiSource`] merges several of them, deduplicated, and
//! tracks how they diverge.

pub mod convert;
pub mod kafka;
pub mod matching;
pub mod multi;
pub mod rpc;
pub mod websocket;

pub use kafka::{KafkaConfig, KafkaSource};
pub use multi::{ChildConfig, ChildSourceConfig, MultiConfig, MultiSource};
pub use shipstern_yellowstone_grpc_source::{YellowstoneGrpcConfig, YellowstoneGrpcSource};
pub use websocket::{WebsocketConfig, WebsocketSource};
