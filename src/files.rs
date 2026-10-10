//! 文件内容按段走 HTTPS（只有密文）：
//!
//! - `PUT /v1/files/{编号}`，请求头 `Upload-Offset: 已传字节数`，请求体是接下来的一段（≤ 8 MiB）；
//! - `HEAD /v1/files/{编号}`：回 `Upload-Offset` 与 `Upload-Length`，断线后从这里续传；
//! - `GET /v1/files/{编号}`：下载，支持 `Range: bytes=起-止`，断点续下。
//!
//! 都要带 `Authorization: Bearer <登录凭证的 base64url>`。

use std::sync::Arc;

use axum::body::{Body, Bytes as BodyBytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use friend_im_core::bytes;
use friend_im_core::ids::FileId;
use friend_im_core::wire::{ErrorCode, WireError};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::ops_account::Session;
use crate::store::fail;
use crate::App;

fn status(error: &WireError) -> StatusCode {
    match error.code {
        ErrorCode::Unauthorized | ErrorCode::NotApproved | ErrorCode::DeviceRemoved => StatusCode::UNAUTHORIZED,
        ErrorCode::Forbidden => StatusCode::FORBIDDEN,
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::Conflict => StatusCode::CONFLICT,
        ErrorCode::LimitExceeded => StatusCode::PAYLOAD_TOO_LARGE,
        ErrorCode::BadRequest | ErrorCode::Invalid => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_response(error: WireError) -> Response {
    (status(&error), axum::Json(error)).into_response()
}

fn bearer(headers: &HeaderMap) -> Result<Vec<u8>, WireError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .and_then(bytes::decode)
        .ok_or_else(|| fail(ErrorCode::Unauthorized, "缺少登录凭证"))
}

async fn session(app: &Arc<App>, headers: &HeaderMap) -> Result<Session, WireError> {
    let token = bearer(headers)?;
    app.with_store(move |locked, _| locked.store.http_session(&token)).await
}

fn parse_file(file: &str) -> Result<FileId, WireError> {
    FileId::parse(file).map_err(|_| fail(ErrorCode::NotFound, "没有这个文件"))
}

pub(crate) async fn progress(State(app): State<Arc<App>>, Path(file): Path<String>, headers: HeaderMap) -> Response {
    let result = async {
        let s = session(&app, &headers).await?;
        let file = parse_file(&file)?;
        app.with_store(move |locked, _| locked.store.file_progress(&s, &file)).await
    }
    .await;
    match result {
        Ok((received, size)) => {
            let mut response = StatusCode::OK.into_response();
            response.headers_mut().insert("Upload-Offset", HeaderValue::from(received));
            response.headers_mut().insert("Upload-Length", HeaderValue::from(size));
            response
        }
        Err(error) => error_response(error),
    }
}

pub(crate) async fn upload(State(app): State<Arc<App>>, Path(file): Path<String>, headers: HeaderMap, body: BodyBytes) -> Response {
    let result = async {
        let s = session(&app, &headers).await?;
        let file = parse_file(&file)?;
        let offset: u64 = headers
            .get("Upload-Offset")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| fail(ErrorCode::BadRequest, "缺少 Upload-Offset"))?;
        let len = body.len() as u64;
        let slot = app
            .with_store(move |locked, _| {
                let Locked { store, uploads, .. } = locked;
                store.upload_begin(uploads, &s, &file, offset, len)
            })
            .await?;
        let written = write_part(&slot.path, slot.offset, &body).await;
        let wrote = if written.is_ok() { len } else { 0 };
        if let Err(error) = &written {
            tracing::warn!(file = %slot.file, %error, "写文件失败");
        }
        let received = app
            .with_store(move |locked, _| {
                let Locked { store, uploads, .. } = locked;
                store.upload_end(uploads, &slot, wrote)
            })
            .await?;
        written.map_err(|_| crate::store::internal("写文件失败"))?;
        Ok::<_, WireError>(received)
    }
    .await;
    match result {
        Ok(received) => {
            let mut response = StatusCode::NO_CONTENT.into_response();
            response.headers_mut().insert("Upload-Offset", HeaderValue::from(received));
            response
        }
        Err(error) => error_response(error),
    }
}

use crate::Locked;

async fn write_part(path: &std::path::Path, offset: u64, data: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let mut file = tokio::fs::OpenOptions::new().create(true).write(true).truncate(false).open(path).await?;
    // 上一段写到一半断了的话，文件可能比记录的长：先截到记录的位置再接着写。
    file.set_len(offset).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    file.flush().await?;
    file.sync_data().await
}

fn parse_range(headers: &HeaderMap, size: u64) -> Option<(u64, u64)> {
    let value = headers.get(header::RANGE)?.to_str().ok()?;
    let spec = value.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: u64 = if end.trim().is_empty() { size.checked_sub(1)? } else { end.trim().parse().ok()? };
    (start <= end && end < size).then_some((start, end))
}

pub(crate) async fn download(State(app): State<Arc<App>>, Path(file): Path<String>, headers: HeaderMap) -> Response {
    let result = async {
        let s = session(&app, &headers).await?;
        let file = parse_file(&file)?;
        app.with_store(move |locked, _| locked.store.download(&s, &file)).await
    }
    .await;
    let (path, size) = match result {
        Ok(found) => found,
        Err(error) => return error_response(error),
    };
    let mut handle = match tokio::fs::File::open(&path).await {
        Ok(handle) => handle,
        Err(error) => {
            tracing::warn!(%error, "打不开要下载的文件");
            return error_response(crate::store::internal("打不开文件"));
        }
    };
    let range = parse_range(&headers, size);
    let (start, end, code) = match range {
        Some((start, end)) => (start, end, StatusCode::PARTIAL_CONTENT),
        None if headers.contains_key(header::RANGE) => {
            let mut response = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            response.headers_mut().insert(header::CONTENT_RANGE, HeaderValue::from_str(&format!("bytes */{size}")).expect("ASCII"));
            return response;
        }
        None => (0, size.saturating_sub(1), StatusCode::OK),
    };
    if start > 0 && handle.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return error_response(crate::store::internal("读文件失败"));
    }
    let length = if size == 0 { 0 } else { end - start + 1 };
    let reader = tokio::io::AsyncReadExt::take(handle, length);
    let stream = tokio_util::io::ReaderStream::new(reader);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = code;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if code == StatusCode::PARTIAL_CONTENT {
        headers.insert(header::CONTENT_RANGE, HeaderValue::from_str(&format!("bytes {start}-{end}/{size}")).expect("ASCII"));
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_parse_and_reject_out_of_bounds() {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=10-"));
        assert_eq!(parse_range(&headers, 100), Some((10, 99)));
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=10-19"));
        assert_eq!(parse_range(&headers, 100), Some((10, 19)));
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=10-200"));
        assert_eq!(parse_range(&headers, 100), None);
        headers.insert(header::RANGE, HeaderValue::from_static("items=1-2"));
        assert_eq!(parse_range(&headers, 100), None);
    }
}
