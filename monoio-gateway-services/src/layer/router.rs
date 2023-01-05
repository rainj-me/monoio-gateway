use std::{borrow::Borrow, cell::UnsafeCell, collections::HashMap, future::Future, rc::Rc};

use async_channel::Receiver;
use http::StatusCode;
use log::{debug, info};
use monoio::{
    io::{
        sink::{Sink, SinkExt},
        stream::Stream,
        AsyncReadRent, AsyncWriteRent, Split, Splitable,
    },
    net::TcpStream,
};
use monoio_gateway_core::{
    dns::{http::Domain, Resolvable},
    error::GError,
    http::{
        router::{RouterConfig, RouterRule},
        Rewrite,
    },
    service::Service,
    transfer::{copy_response_lock, generate_response},
    ACME_URI_PREFIX,
};
use monoio_http::{
    common::{request::Request, response::Response},
    h1::{
        codec::{decoder::RequestDecoder, encoder::GenericEncoder},
        payload::Payload,
    },
};
use monoio_http_client::Client;

use crate::layer::endpoint::ConnectEndpoint;

use super::{
    accept::Accept,
    endpoint::{ClientConnectionType, EndpointRequestParams},
    tls::TlsAccept,
};

pub type SharedTcpConnectPool<I, O> =
    Rc<UnsafeCell<HashMap<String, Rc<ClientConnectionType<I, O>>>>>;

pub struct RouterService<A, I, O: AsyncWriteRent> {
    routes: Rc<RouterConfig<A>>,
    connect_pool: SharedTcpConnectPool<I, O>,
    client: Rc<Client>,
}

impl<A, I, O> Clone for RouterService<A, I, O>
where
    O: AsyncWriteRent,
{
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
            connect_pool: self.connect_pool.clone(),
            client: self.client.clone(),
        }
    }
}

/// Direct use router before Accept
impl<S> Service<Accept<S>> for RouterService<Domain, TcpStream, TcpStream>
where
    S: Split + AsyncReadRent + AsyncWriteRent + 'static,
{
    type Response = ();

    type Error = GError;

    type Future<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + 'a
    where
        Self: 'a;

    fn call(&mut self, incoming_stream: Accept<S>) -> Self::Future<'_> {
        async move {
            let (stream, socketaddr) = incoming_stream;
            let (incoming_read, incoming_write) = stream.into_split();
            let mut incoming_req_decoder = RequestDecoder::new(incoming_read);
            let incoming_resp_encoder =
                Rc::new(UnsafeCell::new(GenericEncoder::new(incoming_write)));
            // let (tx, rx) = async_channel::bounded(1);
            loop {
                let _connect_pool = self.connect_pool.clone();
                match incoming_req_decoder.next().await {
                    Some(Ok(req)) => {
                        let m = longest_match(req.uri().path(), self.routes.get_rules());
                        if let Some(rule) = m {
                            // parsed rule for this request and spawn task to handle endpoint connection
                            let proxy_pass = rule.get_proxy_pass().to_owned();
                            let mut req = req;
                            Rewrite::rewrite_request(&mut req, &proxy_pass);
                            let uri = req.uri().clone();
                            let client = self.client.clone();
                            let incoming_resp_encoder = incoming_resp_encoder.clone();
                            monoio::spawn(async move {
                                match client.send(req).await {
                                    Ok(resp) => {
                                        let incoming_resp_encoder =
                                            unsafe { &mut *incoming_resp_encoder.get() };
                                        let _ = incoming_resp_encoder.send_and_flush(resp).await;
                                    }
                                    Err(e) => {
                                        debug!("send request to {:?} with error:  {:?}", uri, e);
                                        handle_request_error(
                                            incoming_resp_encoder.clone(),
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                        )
                                        .await;
                                    }
                                }
                            });
                            break;
                        } else {
                            debug!("no matching router rule for request uri:  {}", req.uri());
                            handle_request_error(
                                incoming_resp_encoder.clone(),
                                StatusCode::NOT_FOUND,
                            )
                            .await;
                        }
                    }
                    Some(Err(err)) => {
                        // TODO: fallback to tcp
                        log::warn!("{}", err);
                        break;
                    }
                    None => {
                        info!("http client {} closed", socketaddr);
                        break;
                    }
                }
            }
            // notify disconnect from endpoints
            // rx.close();
            // let _ = tx.send(()).await;
            Ok(())
        }
    }
}

/// Direct use router before Accept
///
/// TODO: less copy code
impl<S> Service<TlsAccept<S>> for RouterService<Domain, TcpStream, TcpStream>
where
    S: Split + AsyncReadRent + AsyncWriteRent + 'static,
{
    type Response = ();

    type Error = GError;

    type Future<'a> = impl Future<Output = Result<Self::Response, Self::Error>> + 'a
    where
        Self: 'a;

    fn call(&mut self, incoming_stream: TlsAccept<S>) -> Self::Future<'_> {
        async move {
            let (stream, socketaddr, _) = incoming_stream;
            let (incoming_read, incoming_write) = stream.split();
            let mut incoming_req_decoder = RequestDecoder::new(incoming_read);
            let incoming_resp_encoder =
                Rc::new(UnsafeCell::new(GenericEncoder::new(incoming_write)));
            // exit notifier
            let (tx, rx) = async_channel::bounded(1);
            loop {
                let _connect_pool = self.connect_pool.clone();
                match incoming_req_decoder.next().await {
                    Some(Ok(req)) => {
                        let m = longest_match(req.uri().path(), self.routes.get_rules());
                        if let Some(rule) = m {
                            // parsed rule for this request and spawn task to handle endpoint connection
                            let proxy_pass = rule.get_proxy_pass().to_owned();
                            handle_endpoint_connection_directly(
                                &proxy_pass,
                                incoming_resp_encoder.clone(),
                                req,
                                rx.clone(),
                            )
                            .await;
                            continue;
                        } else {
                            debug!("no matching router rule for request uri: {}", req.uri());
                            handle_request_error(
                                incoming_resp_encoder.clone(),
                                StatusCode::NOT_FOUND,
                            )
                            .await;
                        }
                    }
                    Some(Err(err)) => {
                        log::warn!("{}", err);
                        break;
                    }
                    None => {
                        info!("https client {} closed", socketaddr);
                        break;
                    }
                }
            }
            log::info!("bye {}! Now we remove router", socketaddr);
            // notify disconnect from endpoints
            rx.close();
            let _ = tx.send(()).await;
            Ok(())
        }
    }
}

impl<A, I, O> RouterService<A, I, O>
where
    A: Resolvable,
    O: AsyncWriteRent,
{
    pub fn new(routes: Rc<RouterConfig<A>>) -> Self {
        Self {
            routes,
            connect_pool: Default::default(),
            client: Rc::new(Client::new()),
        }
    }
}

#[inline]
fn longest_match<'cx>(
    req_path: &'cx str,
    routes: &'cx Vec<RouterRule<Domain>>,
) -> Option<&'cx RouterRule<Domain>> {
    log::info!("request path: {}", req_path);
    // TODO: opt progress
    if req_path.starts_with(ACME_URI_PREFIX) {
        return None;
    }
    let mut target_route = None;
    let mut route_len = 0;
    for route in routes.iter() {
        let route_path = route.get_path();
        let route_path_len = route_path.len();
        if req_path.starts_with(route_path) && route_path_len > route_len {
            target_route = Some(route);
            route_len = route_path_len;
        }
    }
    target_route
}

#[cfg(dead_code)]
#[inline]
fn get_host(req: &Request<Payload>) -> Option<&str> {
    match req.headers().get("host") {
        Some(host) => Some(host.to_str().unwrap_or("")),
        None => None,
    }
}

async fn handle_request_error<O>(
    incoming_resp_encoder: Rc<UnsafeCell<GenericEncoder<O>>>,
    status: StatusCode,
) where
    O: AsyncWriteRent + 'static,
    GenericEncoder<O>: monoio::io::sink::Sink<Response<Payload>>,
{
    let incoming_resp_encoder = unsafe { &mut *incoming_resp_encoder.get() };
    let _ = incoming_resp_encoder
        .send_and_flush(generate_response(status))
        .await;
    let _ = incoming_resp_encoder.close().await;
}

/// handl backward connections directly without connection pool
async fn handle_endpoint_connection_directly<O>(
    proxy_pass: &Domain,
    incoming_resp_encoder: Rc<UnsafeCell<GenericEncoder<O>>>,
    mut request: Request<Payload>,
    rx: Receiver<()>,
) where
    O: AsyncWriteRent + 'static,
    GenericEncoder<O>: monoio::io::sink::Sink<Response<Payload>>,
{
    log::info!("connect to upstream host: {}.", proxy_pass.host());
    // open connection
    let mut connect_svc = ConnectEndpoint::default();
    if let Ok(Some(conn)) = connect_svc
        .call(EndpointRequestParams {
            endpoint: proxy_pass.clone(),
        })
        .await
    {
        let connection = Rc::new(conn);
        {
            let conn = connection.clone();
            let proxy_pass_domain = proxy_pass.clone();
            // let local_encoder_clone = encoder.clone();
            let rx_clone = rx.clone();
            monoio::spawn(async move {
                match conn.borrow() {
                    ClientConnectionType::Http(outgoing_resp_reader, _) => {
                        //let cloned = encoder.clone();
                        monoio::select! {
                            _ = copy_response_lock(outgoing_resp_reader.clone(), incoming_resp_encoder.clone(), proxy_pass_domain) => {}
                            _ = rx_clone.recv() => {
                                log::info!("client exit, now cancelling endpoint connection");
                                handle_request_error(incoming_resp_encoder.clone(), StatusCode::INTERNAL_SERVER_ERROR).await;
                            }
                        };
                    }
                    ClientConnectionType::Tls(outgoing_resp_reader, _) => {
                        monoio::select! {
                            _ = copy_response_lock(outgoing_resp_reader.clone(),  incoming_resp_encoder.clone(), proxy_pass_domain) => {}
                            _ = rx_clone.recv() => {
                                log::info!("client exit, now cancelling endpoint connection");
                                handle_request_error(incoming_resp_encoder.clone(), StatusCode::INTERNAL_SERVER_ERROR).await;
                            }
                        };
                    }
                }
            });
        }
        {
            let conn = connection.clone();
            Rewrite::rewrite_request(&mut request, proxy_pass);
            monoio::spawn(async move {
                match conn.borrow() {
                    ClientConnectionType::Http(_, outgoing_req_writer) => {
                        let outgoing_req_writer = unsafe { &mut *outgoing_req_writer.get() };
                        let _ = outgoing_req_writer.send_and_flush(request).await;
                    }
                    ClientConnectionType::Tls(_, outgoing_req_writer) => {
                        let outgoing_req_writer = unsafe { &mut *outgoing_req_writer.get() };
                        let _ = outgoing_req_writer.send_and_flush(request).await;
                    }
                }
            });
        }
    } else {
        handle_request_error(incoming_resp_encoder, StatusCode::NOT_FOUND).await;
    }
}

#[cfg(dead_code)]
/// handle backward connections and send request to endpoint.
/// This function use spawn feature of monoio and will not block caller.
async fn handle_endpoint_connection<O>(
    connect_pool: SharedTcpConnectPool<TcpStream, TcpStream>,
    proxy_pass: &Domain,
    encoder: Rc<UnsafeCell<GenericEncoder<O>>>,
    mut request: Request<Payload>,
    rx: Receiver<()>,
) where
    O: AsyncWriteRent + 'static,
    GenericEncoder<O>: monoio::io::sink::Sink<Response<Payload>>,
{
    // we add a write lock to prevent multiple context execute into block below.
    {
        let connect_pool = unsafe { &mut *connect_pool.get() };
        // critical code start
        if !connect_pool.contains_key(proxy_pass.host()) {
            // hold endpoint request, prevent
            log::info!(
                "{} endpoint connections not exists, try connect now. [{:?}]",
                proxy_pass.host(),
                connect_pool.keys()
            );
            // open channel
            let proxy_pass_domain = proxy_pass.clone();
            let local_encoder_clone = encoder.clone();
            // no connections
            let mut connect_svc = ConnectEndpoint::default();
            if let Ok(Some(conn)) = connect_svc
                .call(EndpointRequestParams {
                    endpoint: proxy_pass.clone(),
                })
                .await
            {
                let conn = Rc::new(conn);
                connect_pool.insert(proxy_pass_domain.host().to_owned(), conn.clone());
                // endpoint -> proxy -> client
                let _connect_pool_cloned = connect_pool.clone();
                let rx_clone = rx.clone();
                monoio::spawn(async move {
                    match conn.borrow() {
                        ClientConnectionType::Http(i, _) => {
                            let cloned = local_encoder_clone.clone();
                            monoio::select! {
                                _ = copy_response_lock(i.clone(), local_encoder_clone, proxy_pass_domain.clone()) => {}
                                _ = rx_clone.recv() => {
                                    log::info!("client exit, now cancelling endpoint connection");
                                    handle_request_error(cloned, StatusCode::INTERNAL_SERVER_ERROR).await;
                                }
                            };
                        }
                        ClientConnectionType::Tls(i, _) => {
                            let cloned = local_encoder_clone.clone();
                            monoio::select! {
                                _ = copy_response_lock(i.clone(), local_encoder_clone, proxy_pass_domain.clone()) => {}
                                _ = rx_clone.recv() => {
                                    log::info!("client exit, now cancelling endpoint connection");
                                    handle_request_error(cloned, StatusCode::INTERNAL_SERVER_ERROR).await;
                                }
                            };
                        }
                    }
                    // remove proxy pass endpoint
                    connect_pool.remove(proxy_pass_domain.host());
                    log::info!("🗑 remove {} from endpoint pool", &proxy_pass_domain);
                });
            } else {
                handle_request_error(encoder, StatusCode::NOT_FOUND).await;
            }
        } else {
            log::info!("🚀 endpoint connection found for {}!", proxy_pass);
        }
    }
    let connect_pool = unsafe { &mut *connect_pool.get() };
    if let Some(conn) = connect_pool.get(proxy_pass.host()) {
        // send this request to endpoint
        let conn = conn.clone();
        let proxy_pass_domain = proxy_pass.clone();
        monoio::spawn(async move {
            Rewrite::rewrite_request(&mut request, &proxy_pass_domain);
            match conn.borrow() {
                ClientConnectionType::Http(_, sender) => {
                    let sender = unsafe { &mut *sender.get() };
                    let _ = sender.send_and_flush(request).await;
                }
                ClientConnectionType::Tls(_, sender) => {
                    let sender = unsafe { &mut *sender.get() };
                    let _ = sender.send_and_flush(request).await;
                }
            }
        });
    }
}
