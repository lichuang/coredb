// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::LazyLock;

use hickory_resolver::TokioResolver;

use crate::errors::Error;
use crate::errors::Result;

pub struct DNSResolver {
  inner: TokioResolver,
}

static INSTANCE: LazyLock<Result<Arc<DNSResolver>>> =
  LazyLock::new(|| match TokioResolver::builder_tokio() {
    Err(error) => Err(Error::dns_parse_error(format!(
      "DNS resolver create error: {}",
      error
    ))),
    Ok(resolver) => Ok(Arc::new(DNSResolver {
      inner: resolver.build(),
    })),
  });

impl DNSResolver {
  pub fn instance() -> Result<Arc<DNSResolver>> {
    match INSTANCE.as_ref() {
      Ok(resolver) => Ok(resolver.clone()),
      Err(error) => Err(Error::dns_parse_error(error.to_string())),
    }
  }

  pub async fn resolve(&self, hostname: impl Into<String>) -> Result<Vec<IpAddr>> {
    let hostname = hostname.into();
    match self.inner.lookup_ip(hostname.clone()).await {
      Ok(lookup_ip) => Ok(lookup_ip.iter().collect::<Vec<_>>()),
      Err(error) => Err(Error::dns_parse_error(format!(
        "Cannot lookup ip {} : {}",
        hostname, error
      ))),
    }
  }
}
