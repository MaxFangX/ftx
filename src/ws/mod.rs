//! This module is used to interact with the Websocket API.

mod error;
mod model;
#[cfg(test)]
mod tests;

pub use error::*;
pub use model::*;

use futures_util::{sink, Sink};
use futures_util::stream::{FuturesUnordered, SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use hmac_sha256::HMAC;
use serde_json::json;
use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::time; // 1.3.0
use tokio::time::Interval;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub struct Ws {
    stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    write_rx: Sender<Message>,
    sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    buf: VecDeque<Data>,
    ping_timer: Interval,
    pings: FuturesUnordered<
        sink::Send<
            'static,
            SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
            Message,
        >,
    >,
}

impl Ws {
    pub const ENDPOINT: &'static str = "wss://ftx.com/ws";
    pub const ENDPOINT_US: &'static str = "wss://ftx.us/ws";

    async fn connect_with_endpoint(
        endpoint: &str,
        key: String,
        secret: String,
        subaccount: Option<String>,
    ) -> Result<Self> {
        let (stream, _) = connect_async(endpoint).await?;

        let (mut sink, stream) = stream.split();

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let sign_payload = format!("{}websocket_login", timestamp);
        let sign = HMAC::mac(sign_payload.as_bytes(), secret.as_bytes());
        let sign = hex::encode(sign);

        sink.send(Message::Text(
            json!({
                "op": "login",
                "args": {
                    "key": key,
                    "sign": sign,
                    "time": timestamp as u64,
                    "subaccount": subaccount,
                }
            })
            .to_string(),
        ))
        .await?;

        Ok(Self {
            stream,
            sink,
            buf: VecDeque::new(),
            ping_timer: time::interval(Duration::from_secs(15)),
            pings: FuturesUnordered::new(),
        })
    }

    pub async fn connect(key: String, secret: String, subaccount: Option<String>) -> Result<Self> {
        Self::connect_with_endpoint(Self::ENDPOINT, key, secret, subaccount).await
    }

    pub async fn connect_us(
        key: String,
        secret: String,
        subaccount: Option<String>,
    ) -> Result<Self> {
        Self::connect_with_endpoint(Self::ENDPOINT_US, key, secret, subaccount).await
    }

    async fn ping(&mut self) -> Result<()> {
        self.sink
            .send(Message::Text(
                json!({
                    "op": "ping",
                })
                .to_string(),
            ))
            .await?;

        Ok(())
    }

    pub async fn subscribe(&mut self, channels: Vec<Channel>) -> Result<()> {
        for channel in channels {
            let (channel, symbol) = match channel {
                Channel::Orderbook(symbol) => ("orderbook", symbol),
                Channel::Trades(symbol) => ("trades", symbol),
                Channel::Ticker(symbol) => ("ticker", symbol),
                Channel::Fills => ("fills", "".to_string()),
            };

            self.sink
                .send(Message::Text(
                    json!({
                        "op": "subscribe",
                        "channel": channel,
                        "market": symbol,
                    })
                    .to_string(),
                ))
                .await?;

            match self.next_response().await? {
                Response {
                    r#type: Type::Subscribed,
                    ..
                } => {}
                _ => return Err(Error::MissingSubscriptionConfirmation),
            }
        }

        Ok(())
    }

    async fn next_response(&mut self) -> Result<Response> {
        loop {
            tokio::select! {
                _ = self.ping_timer.tick() => {
                    // self.ping() has type Future<Output=Result<()>>
                    // self.pings.push(self.ping().boxed());
                    // self.ping().await?;

                    // Send<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>, Message>
                    // NOTE Unsure how to use this future
                    let ping = self.sink
                        .send(Message::Text(
                            json!({
                                "op": "ping",
                            })
                            .to_string(),
                        ));

                    // FIXME: Uncomment to resolve compiler error
                    self.pings.push(ping);

                    // NOTE No into_split() function available, can't clone or move
                    // FIXME: This doesn't work either
                    tokio::spawn(async move {
                        ping.await?;

                        Ok::<_, Error>(())
                    });

                    // TODO select on the ping futures
                },
                Some(msg) = self.stream.next() => {
                    let msg = msg?;
                    if let Message::Text(text) = msg {
                        // println!("Stream: {}", text); // Uncomment for debugging
                        let response: Response = serde_json::from_str(&text)?;

                        // Don't return Pong responses
                        if let Response { r#type: Type::Pong, .. } = response {
                            continue;
                        }

                        return Ok(response)
                    }
                },
                // Some(msg) = self.sink => {
                //     let msg = msg?;
                //     if let Message::Text(text) = msg {
                //         println!("Sink: {}", text); // Uncomment for debugging
                //
                //         // TODO do I need to do something with this
                //     }
                // }
            }
        }
    }

    /// Helper function that takes a response and adds the contents to the buffer
    fn handle_response(&mut self, response: Response) -> Result<()> {
        if let Some(data) = response.data {
            match data {
                ResponseData::Trades(trades) => {
                    // Trades channel returns an array of single trades.
                    // Buffer so that the user receives trades one at a time
                    for trade in trades {
                        self.buf.push_back(Data::Trade(trade));
                    }
                }
                ResponseData::OrderbookData(orderbook) => {
                    self.buf.push_back(Data::OrderbookData(orderbook));
                }
                ResponseData::Fill(fill) => {
                    self.buf.push_back(Data::Fill(fill));
                }
            }
        }

        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<Data>> {
        loop {
            // If buffer contains data, we can directly return it.
            if let Some(data) = self.buf.pop_front() {
                return Ok(Some(data));
            }

            // Fetch new response if buffer is empty.
            let response = self.next_response().await?;

            // Handle the response, possibly adding to the buffer
            self.handle_response(response)?;
        }
    }
}
