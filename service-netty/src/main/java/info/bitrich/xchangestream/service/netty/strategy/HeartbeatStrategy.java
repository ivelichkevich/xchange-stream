package info.bitrich.xchangestream.service.netty.strategy;

import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.time.Duration;

/**
 * Strategy to send periodic pings/heartbeats to maintain connection
 */
public interface HeartbeatStrategy {

    /**
     * Message sending interval
     */
    Duration getPeriod();

    /**
     * String to send as heartbeat. Can be null
     */
    default String getHeartbeatString() {
        return null;
    }

    /**
     * If heartbeat string is not defined, send WebSocket PING
     */
    default WebSocketFrame getHeartbeatFrame() {
        String heartbeat = getHeartbeatString();
        if (heartbeat == null) {
            return new PingWebSocketFrame();
        }

        return new TextWebSocketFrame(heartbeat);
    }
}
