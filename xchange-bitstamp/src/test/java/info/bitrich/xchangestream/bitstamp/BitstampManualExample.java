package info.bitrich.xchangestream.bitstamp;

import static info.bitrich.xchangestream.core.StreamingExchangeFactory.INSTANCE;
import static org.knowm.xchange.currency.CurrencyPair.BTC_USD;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BitstampManualExample {

    private static final Logger LOG = LoggerFactory.getLogger(BitstampManualExample.class);

    public static void main(String[] args) {

        StreamingExchange exchange = INSTANCE.createExchange(BitstampStreamingExchange.class.getName());
        exchange.connect().blockingAwait();

        exchange.getStreamingMarketDataService().getOrderBook(BTC_USD).subscribe(orderBook -> {
            LOG.info("First ask: {}", orderBook.getAsks().get(0));
            LOG.info("First bid: {}", orderBook.getBids().get(0));
        });

        Disposable subscribe = exchange.getStreamingMarketDataService().getTrades(BTC_USD)
                .subscribe(trade -> LOG.info("Trade {}", trade));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        subscribe.dispose();
        exchange.disconnect().subscribe(() -> LOG.info("Disconnected from the Exchange"));
    }
}
