//! Real gRPC transport regressions against a local single-method service.

use std::{
    convert::Infallible,
    task::{Context, Poll},
    time::Duration,
};

use bigtable_client::{
    Client, ClientConfig,
    proto::{
        ReadRowsRequest, ReadRowsResponse,
        read_rows_response::{CellChunk, cell_chunk::RowStatus},
    },
};
use bytes::Bytes;
use prost::Message;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Request, Response, Status,
    body::Body,
    codegen::{BoxFuture, BoxStream, Service},
    server::{Grpc, ServerStreamingService},
    transport::Server,
};

#[derive(Clone)]
struct ReadRowsService(ReadRowsResponse);

impl ServerStreamingService<ReadRowsRequest> for ReadRowsService {
    type Response = ReadRowsResponse;
    type ResponseStream = BoxStream<ReadRowsResponse>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, _: Request<ReadRowsRequest>) -> Self::Future {
        let stream: Self::ResponseStream = Box::pin(tokio_stream::iter([Ok(self.0.clone())]));
        Box::pin(async move { Ok(Response::new(stream)) })
    }
}

impl Service<http::Request<Body>> for ReadRowsService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/google.bigtable.v2.Bigtable/ReadRows"
        );
        let service = self.clone();
        Box::pin(async move {
            Ok(Grpc::new(tonic_prost::ProstCodec::default())
                .server_streaming(service, request)
                .await)
        })
    }
}

#[tokio::test]
async fn reads_a_response_larger_than_tonics_default_receive_limit() {
    let value = Bytes::from(vec![b'x'; 5 * 1024 * 1024]);
    let response = ReadRowsResponse {
        chunks: vec![CellChunk {
            row_key: Bytes::from_static(b"large-row"),
            family_name: Some("cf".to_owned()),
            qualifier: Some(b"q".to_vec()),
            value: value.clone(),
            row_status: Some(RowStatus::CommitRow(true)),
            ..CellChunk::default()
        }],
        ..ReadRowsResponse::default()
    };
    assert!(response.encoded_len() > 4 * 1024 * 1024);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("available local port");
    let address = listener.local_addr().expect("bound address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = Server::builder().serve_with_incoming_shutdown(
        ReadRowsService(response),
        TcpListenerStream::new(listener),
        async {
            let _ = shutdown_rx.await;
        },
    );
    let read = async {
        let result = async {
            let config = ClientConfig::new("project", "instance")?
                .with_emulator_host(address.to_string())?;
            let client = Client::connect(config).await?;
            client.read_row("table", b"large-row".to_vec()).await
        }
        .await;
        let _ = shutdown_tx.send(());
        result
    };
    let (server_result, row_result) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(server, read) })
            .await
            .expect("local request and server shutdown complete");
    server_result.expect("server shuts down cleanly");
    let row = row_result
        .expect("large response is accepted")
        .expect("row exists");

    assert_eq!(row.key, b"large-row".as_slice());
    assert_eq!(row.families[0].columns[0].cells[0].value, value);
}
