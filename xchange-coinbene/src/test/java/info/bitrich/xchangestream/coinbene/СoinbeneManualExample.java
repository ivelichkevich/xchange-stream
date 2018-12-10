package info.bitrich.xchangestream.coinbene;

import static info.bitrich.xchangestream.core.StreamingExchangeFactory.INSTANCE;

import info.bitrich.xchangestream.core.StreamingExchange;
import io.reactivex.disposables.Disposable;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class СoinbeneManualExample {
    private static final Logger LOG = LoggerFactory.getLogger(СoinbeneManualExample.class);

    public static void main(String[] args) {

        StreamingExchange exchange = INSTANCE.createExchange(CoinbeneStreamingExchange.class.getName());
        //exchange.connect().blockingAwait();

        Disposable disposable = exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.ETH_BTC)
                .subscribe(orderBook -> {
                    LOG.info("First ask: {}", orderBook.getAsks().get(0));
                    LOG.info("First bid: {}", orderBook.getBids().get(0));
                });

        exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.LTC_BTC).subscribe(orderBook -> {
            LOG.info("First ask: {}", orderBook.getAsks().get(0));
            LOG.info("First bid: {}", orderBook.getBids().get(0));
        });

        StreamingExchange exchange2 = INSTANCE.createExchange(CoinbeneStreamingExchange.class.getName());

        exchange2.getStreamingMarketDataService().getOrderBook(CurrencyPair.OMG_BTC).subscribe(orderBook -> {
            LOG.info("First ask: {}", orderBook.getAsks().get(0));
            LOG.info("First bid: {}", orderBook.getBids().get(0));
        });

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        disposable.dispose();
        LOG.info("\n!!!!!! dispose ETH_BTC !!!!!!!!");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        exchange.disconnect();
        LOG.info("\n!!!!!! exchange.disconnect !!!!!!!!");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        exchange2.disconnect();
        LOG.info("\n!!!!!! exchange2.disconnect !!!!!!!!");

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
