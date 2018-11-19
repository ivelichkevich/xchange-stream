package info.bitrich.xchangestream.coinbene;

import static info.bitrich.xchangestream.core.StreamingExchangeFactory.INSTANCE;

import info.bitrich.xchangestream.core.StreamingExchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class СoinbeneManualExample {
    private static final Logger LOG = LoggerFactory.getLogger(СoinbeneManualExample.class);

    public static void main(String[] args) {

        StreamingExchange exchange = INSTANCE.createExchange(CoinbeneStreamingExchange.class.getName());
        exchange.connect().blockingAwait();

        exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.ETH_BTC).subscribe(orderBook -> {
            LOG.info("First ask: {}", orderBook.getAsks().get(0));
            LOG.info("First bid: {}", orderBook.getBids().get(0));
        });

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
