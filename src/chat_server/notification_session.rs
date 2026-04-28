use actix::prelude::*;
use actix_web_actors::ws;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::message::*;
use super::server::ChatServer;

pub struct NotificationSession {
    pub user_id: Uuid,
    pub connection_id: Uuid,
    pub server_addr: Addr<ChatServer>,
    pub hb: Instant,
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(45);

impl NotificationSession {
    fn start_heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |actor, ctx| {
            if Instant::now().duration_since(actor.hb) > CLIENT_TIMEOUT {
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }
}

impl Actor for NotificationSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_heartbeat(ctx);
        let addr = ctx.address();
        self.server_addr.do_send(Connect {
            user_id: self.user_id,
            connection_id: self.connection_id,
            channel: ConnectionChannel::Notifications,
            addr: addr.recipient(),
        });
        ctx.text(format!(
            "{{\"type\":\"connected\",\"channel\":\"notifications\",\"user_id\":\"{}\"}}",
            self.user_id
        ));
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        self.server_addr.do_send(Disconnect {
            user_id: self.user_id,
            connection_id: self.connection_id,
        });
    }
}

impl Handler<ServerMessage> for NotificationSession {
    type Result = ();

    fn handle(&mut self, msg: ServerMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for NotificationSession {
    fn handle(
        &mut self,
        msg: Result<ws::Message, ws::ProtocolError>,
        ctx: &mut Self::Context,
    ) {
        match msg {
            Ok(ws::Message::Ping(payload)) => {
                self.hb = Instant::now();
                ctx.pong(&payload);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => {}
        }
    }
}
