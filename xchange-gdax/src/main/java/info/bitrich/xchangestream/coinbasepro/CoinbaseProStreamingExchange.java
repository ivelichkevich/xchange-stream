package info.bitrich.xchangestream.coinbasepro;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.reactivex.Completable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.coinbasepro.CoinbaseProExchange;

/**
 * CoinbasePro Streaming Exchange. Connects to live WebSocket feed.
 */
public class CoinbaseProStreamingExchange extends CoinbaseProExchange implements StreamingExchange {
    private static final String API_URI = "wss://ws-feed.pro.coinbase.com";

    private CoinbaseProStreamingService streamingService;
    private CoinbaseProStreamingMarketDataService streamingMarketDataService;

    public CoinbaseProStreamingExchange() { }

    @Override
    protected void initServices() {
        super.initServices();
    }

    @Override
    public Completable connect(ProductSubscription... args) {
        if (args == null || args.length == 0)
            throw new UnsupportedOperationException("The ProductSubscription must be defined!");
        this.streamingService = new CoinbaseProStreamingService(API_URI);
        this.streamingMarketDataService = new CoinbaseProStreamingMarketDataService(this.streamingService);
        streamingService.subscribeMultipleCurrencyPairs(args);

        return streamingService.connect();
    }

    @Override
    public Completable disconnect() {
        CoinbaseProStreamingService service = this.streamingService;
        streamingService = null;
        streamingMarketDataService = null;
        return service.disconnect();
    }

    @Override
    public ExchangeSpecification getDefaultExchangeSpecification() {
        ExchangeSpecification spec = super.getDefaultExchangeSpecification();
        spec.setShouldLoadRemoteMetaData(false);

        return spec;
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketDataService;
    }

    /**
     * Enables the user to listen on channel inactive events and react appropriately.
     *
     * @param channelInactiveHandler a WebSocketMessageHandler instance.
     */
    public void setChannelInactiveHandler(WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
        streamingService.setChannelInactiveHandler(channelInactiveHandler);
    }

    @Override
    public boolean isAlive() {
        return streamingService != null && streamingService.isSocketOpen();
    }

    @Override
    public void useCompressedMessages(boolean compressedMessages) { streamingService.useCompressedMessages(compressedMessages); }
}
