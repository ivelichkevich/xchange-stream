package info.bitrich.xchangestream.coinbasepro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.coinbasepro.dto.CoinbaseProWebSocketSubscriptionMessage;
import info.bitrich.xchangestream.coinbasepro.netty.WebSocketClientCompressionAllowClientNoContextHandler;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CoinbaseProStreamingService extends JsonNettyStreamingService {
    private static final Logger LOG = LoggerFactory.getLogger(CoinbaseProStreamingService.class);
    private static final String SUBSCRIBE = "subscribe";
    private static final String UNSUBSCRIBE = "unsubscribe";
    private static final String SHARE_CHANNEL_NAME = "ALL";
    private final Map<String, Observable<JsonNode>> subscriptions = new HashMap<>();
    private ProductSubscription product = null;

    private WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler = null;

    public CoinbaseProStreamingService(String apiUrl) {
        super(apiUrl, Integer.MAX_VALUE);
    }

    public ProductSubscription getProduct() {
        return product;
    }

    @Override
    public String getSubscriptionUniqueId(String channelName, Object... args) {
        return SHARE_CHANNEL_NAME;
    }

    /**
     * Subscribes to the provided channel name, maintains a cache of subscriptions, in order not to
     * subscribe more than once to the same channel.
     *
     * @param channelName the name of the requested channel.
     * @return an Observable of json objects coming from the exchange.
     */
    @Override
    public Observable<JsonNode> subscribeChannel(String channelName, Object... args) {
        channelName = SHARE_CHANNEL_NAME;

        if (!channels.containsKey(channelName) && !subscriptions.containsKey(channelName)) {
            subscriptions.put(channelName, super.subscribeChannel(channelName, args));
        }

        return subscriptions.get(channelName);
    }

    @Override
    protected String getChannelNameFromMessage(JsonNode message) {
        return SHARE_CHANNEL_NAME;
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {
        CoinbaseProWebSocketSubscriptionMessage subscribeMessage = new CoinbaseProWebSocketSubscriptionMessage(SUBSCRIBE, product);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(subscribeMessage);
    }

    @Override
    public String getUnsubscribeMessage(String channelName) throws IOException {
        CoinbaseProWebSocketSubscriptionMessage subscribeMessage =
                new CoinbaseProWebSocketSubscriptionMessage(UNSUBSCRIBE, new String[]{"level2", "matches", "ticker"});
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(subscribeMessage);
    }

    @Override
    protected void handleMessage(JsonNode message) {
        super.handleMessage(message);
    }

    @Override
    protected WebSocketClientExtensionHandler getWebSocketClientExtensionHandler() {
        return WebSocketClientCompressionAllowClientNoContextHandler.INSTANCE;
    }

    @Override
    protected WebSocketClientHandler getWebSocketClientHandler(WebSocketClientHandshaker handshaker,
                                                               WebSocketClientHandler.WebSocketMessageHandler handler) {
        LOG.info("Registering CoinbaseProWebSocketClientHandler");
        return new CoinbaseProWebSocketClientHandler(handshaker, handler);
    }

    public void setChannelInactiveHandler(WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
        this.channelInactiveHandler = channelInactiveHandler;
    }

    public void subscribeMultipleCurrencyPairs(ProductSubscription... products) {
        this.product = products[0];
    }

    /**
     * Custom client handler in order to execute an external, user-provided handler on channel events.
     * This is useful because it seems CoinbasePro unexpectedly closes the web socket connection.
     */
    class CoinbaseProWebSocketClientHandler extends NettyWebSocketClientHandler {

        public CoinbaseProWebSocketClientHandler(WebSocketClientHandshaker handshaker, WebSocketMessageHandler handler) {
            super(handshaker, handler);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            super.channelInactive(ctx);
            if (channelInactiveHandler != null) {
                channelInactiveHandler.onMessage("WebSocket Client disconnected!");
            }
        }
    }
}
