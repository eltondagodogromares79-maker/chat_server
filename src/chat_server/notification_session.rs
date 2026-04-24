use actix::prelude::*;
use actix_web_actors::ws;
use uuid::Uuid;

use super::message::*;
use super::server::ChatServer;

pub struct NotificationSession {
    pub user_id: Uuid,
    pub connection_id: Uuid,
    pub server_addr: Addr<ChatServer>,
}

impl Actor for NotificationSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        self.server_addr.do_send(Connect {
            user_id: self.user_id,
            connection_id: self.connection_id,
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
            Ok(ws::Message::Ping(payload)) => ctx.pong(&payload),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => {}
        }
    }
}
