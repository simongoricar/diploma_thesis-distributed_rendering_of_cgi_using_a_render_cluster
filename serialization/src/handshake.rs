use flatbuffers::FlatBufferBuilder;
use tokio_tungstenite::tungstenite::Message;

use crate::handshake::handshake_request::{
    MasterServerHandshakeRequest,
    MasterServerHandshakeRequestArgs,
    MASTER_SERVER_HANDSHAKE_REQUEST_IDENTIFIER,
};


/*
 * Generated code modules
 */

#[allow(non_snake_case, warnings)]
#[path = "./compiled_definitions/handshake_request_generated.rs"]
pub mod handshake_request;

#[allow(non_snake_case, warnings)]
#[path = "./compiled_definitions/handshake_response_generated.rs"]
pub mod handshake_response;

#[allow(non_snake_case, warnings)]
#[path = "./compiled_definitions/handshake_acknowledgement_generated.rs"]
pub mod handshake_acknowledgement;


/*
 * Helper functions
 */

pub fn new_handshake_request_message<S: AsRef<str>>(
    builder: &mut FlatBufferBuilder,
    server_version: Option<S>,
) -> Message {
    builder.reset();

    let server_version_object =
        server_version.map(|version| builder.create_string(version.as_ref()));

    let handshake_request = MasterServerHandshakeRequest::create(
        builder,
        &MasterServerHandshakeRequestArgs {
            server_version: server_version_object,
        },
    );
    builder.finish(
        handshake_request,
        Some(MASTER_SERVER_HANDSHAKE_REQUEST_IDENTIFIER),
    );

    return Message::binary(builder.finished_data());
}
