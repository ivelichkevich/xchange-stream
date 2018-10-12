package info.bitrich.xchangestream.cexio;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingPrivateDataService;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.knowm.xchange.cexio.CexIOExchange;

public class CexioStreamingExchange extends CexIOExchange implements StreamingExchange {

    private static final String API_URI = "wss://ws.cex.io/ws/";

    private final CexioStreamingMarketDataService streamingMarketDataService;

    private final CexioStreamingService streamingService;
    private final CexioStreamingPrivateDataRawService streamingPrivateDataService;

    public CexioStreamingExchange() {
        this.streamingService = new CexioStreamingService(this, API_URI);

        this.streamingPrivateDataService = new CexioStreamingPrivateDataRawService(streamingService);
        this.streamingMarketDataService = new CexioStreamingMarketDataService(streamingService);
    }

    CexioStreamingExchange(CexioStreamingService streamingService) {
        this.streamingService = streamingService;
        this.streamingPrivateDataService = new CexioStreamingPrivateDataRawService(streamingService);
        this.streamingMarketDataService = new CexioStreamingMarketDataService(streamingService);
    }

    @Override
    public Completable connect(ProductSubscription... args) {
        return streamingService.connect();
    }

    @Override
    public Completable disconnect() {
        return streamingService.disconnect();
    }

    @Override
    public boolean isAlive() {
        return streamingService.isSocketOpen();
    }

    @Override
    public Observable<Boolean> ready() {
        return streamingService.ready();
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketDataService;
    }

    @Override
    public StreamingPrivateDataService getStreamingPrivateDataService() {
        return streamingPrivateDataService;
    }

    @Override
    public void useCompressedMessages(boolean compressedMessages) {
        streamingService.useCompressedMessages(compressedMessages);
    }
}
