package info.bitrich.xchangestream.coinbene;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Completable;
import org.knowm.xchange.coinbene.CoinbeneExchange;

public class CoinbeneStreamingExchange extends CoinbeneExchange implements StreamingExchange {

    private CoinbeneStreamingMarketDataService streamingMarketDataService;

    @Override
    protected void initServices() {
        super.initServices();
        streamingMarketDataService = new CoinbeneStreamingMarketDataService(marketDataService);
    }

    @Override
    public Completable connect(ProductSubscription... args) {
        return Completable.complete();
    }

    @Override
    public Completable disconnect() {
        streamingMarketDataService.disposeAll();
        return Completable.complete();
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketDataService;
    }

    @Override
    public boolean isAlive() {
        return streamingMarketDataService.isAlive();
    }

    @Override
    public void useCompressedMessages(boolean compressedMessages) {
    }
}
