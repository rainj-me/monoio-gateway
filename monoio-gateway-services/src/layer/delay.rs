use std::{future::Future, time::Duration};

use monoio_gateway_core::{
    service::{Layer, Service},
};

pub struct DelayService<T> {
    inner: T,
    delay: Duration,
}

impl<R, T> Service<R> for DelayService<T>
where
    T: Service<R>,
{
    type Response = T::Response;

    type Error = T::Error;

    type Future<'cx> = impl Future<Output = Result<Self::Response, Self::Error>>
    where
        Self: 'cx;

    fn call(&mut self, req: R) -> Self::Future<'_> {
        async move {
            monoio::time::sleep(self.delay.to_owned()).await;

            self.inner.call(req).await
        }
    }
}

pub struct DelayLayer {
    delay: Duration,
}

impl<S> Layer<S> for DelayLayer {
    type Service = DelayService<S>;

    fn layer(&self, service: S) -> Self::Service {
        DelayService {
            inner: service,
            delay: self.delay,
        }
    }
}

impl DelayLayer {
    pub fn new(delay: Duration) -> Self {
        Self { delay }
    }
}
