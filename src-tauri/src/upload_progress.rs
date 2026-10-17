use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use serde::Serialize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize)]
pub struct UploadProgress {
    pub uploaded_bytes: u64,
    pub total_bytes: u64,
}

pub type ProgressCallback = Arc<dyn Fn(UploadProgress) + Send + Sync>;

pub fn track_upload(body: ByteStream, total: u64, callback: ProgressCallback) -> ByteStream {
    // map preserves the SDK's ability to reopen the file on retries. Each new
    // request gets its own counter, so retried bytes never inflate the total.
    body.map(move |inner| {
        callback(UploadProgress {
            uploaded_bytes: 0,
            total_bytes: total,
        });
        SdkBody::from_body_1_x(ProgressBody {
            inner,
            sent: 0,
            total,
            callback: callback.clone(),
            last_update: Instant::now(),
        })
    })
}

struct ProgressBody {
    inner: SdkBody,
    sent: u64,
    total: u64,
    callback: ProgressCallback,
    last_update: Instant,
}

impl Body for ProgressBody {
    type Data = Bytes;
    type Error = <SdkBody as Body>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if let Some(data) = frame.data_ref() {
                self.sent = self.sent.saturating_add(data.len() as u64).min(self.total);
                if self.sent == self.total
                    || self.last_update.elapsed() >= Duration::from_millis(100)
                {
                    (self.callback)(UploadProgress {
                        uploaded_bytes: self.sent,
                        total_bytes: self.total,
                    });
                    self.last_update = Instant::now();
                }
            }
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        Body::size_hint(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[tokio::test]
    async fn tracks_real_file_bytes_preserves_contents_and_resets_on_retry() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let content = vec![42; 256 * 1024];
        std::fs::write(file.path(), &content).unwrap();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let observed = updates.clone();
        let callback: ProgressCallback = Arc::new(move |event| {
            observed.lock().unwrap().push(event);
        });
        let body = track_upload(
            ByteStream::from_path(file.path()).await.unwrap(),
            content.len() as u64,
            callback,
        )
        .into_inner();
        assert_eq!(Body::size_hint(&body).exact(), Some(content.len() as u64));
        let retry = body.try_clone().expect("upload body must remain retryable");
        assert_eq!(
            ByteStream::new(body)
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            &content
        );
        assert_eq!(
            ByteStream::new(retry)
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .as_ref(),
            &content
        );
        let updates = updates.lock().unwrap();
        assert!(
            updates
                .iter()
                .filter(|event| event.uploaded_bytes == 0)
                .count()
                >= 2
        );
        assert_eq!(updates.last().unwrap().uploaded_bytes, content.len() as u64);
        assert!(updates
            .iter()
            .all(|event| event.uploaded_bytes <= event.total_bytes));
    }

    #[tokio::test]
    async fn supports_empty_files() {
        let body = track_upload(
            ByteStream::from_static(b""),
            0,
            Arc::new(|event| {
                assert_eq!(event.uploaded_bytes, 0);
                assert_eq!(event.total_bytes, 0);
            }),
        );
        assert!(body.collect().await.unwrap().into_bytes().is_empty());
    }
}
