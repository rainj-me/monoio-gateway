use http::{uri::Scheme, HeaderValue, Uri};
use monoio_http::{
    common::{request::Request, response::Response},
    h1::payload::Payload,
};

use crate::dns::http::Domain;

pub struct Rewrite;

// TODO: change to rewrite headers.
impl Rewrite {
    #[inline]
    pub fn rewrite_request(request: &mut Request<Payload>, remote: &Domain) {
        match remote.authority() {
            Some(authority) => {
                let header_value = HeaderValue::from_str(authority.as_str())
                    .unwrap_or(HeaderValue::from_static(""));
                log::debug!(
                    "Request: {:?} -> {:?}",
                    request.headers().get(http::header::HOST),
                    header_value
                );
                request
                    .headers_mut()
                    .insert(http::header::HOST, header_value);

                let scheme = match remote.scheme() {
                    Some(scheme) => scheme.to_owned(),
                    None => Scheme::HTTP,
                };

                let uri = request.uri_mut();
                let path_and_query = match uri.path_and_query() {
                    Some(path_and_query) => path_and_query.as_str(),
                    None => "/",
                };
                *uri = Uri::builder()
                    .authority(authority.to_owned())
                    .scheme(scheme)
                    .path_and_query(path_and_query)
                    .build()
                    .unwrap();
            }
            None => return,
        }
    }

    #[inline]
    pub fn rewrite_response(response: &mut Response<Payload>, local: &Domain) {
        match local.authority() {
            Some(authority) => {
                let header_value = HeaderValue::from_str(authority.as_str())
                    .unwrap_or(HeaderValue::from_static(""));
                log::debug!(
                    "Response: {:?} <- {:?}",
                    header_value,
                    response.headers().get(http::header::HOST)
                );
                response
                    .headers_mut()
                    .insert(http::header::HOST, header_value);
            }
            None => return,
        }
    }
}
