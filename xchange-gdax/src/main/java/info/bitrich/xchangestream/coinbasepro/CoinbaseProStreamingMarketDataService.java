package info.bitrich.xchangestream.coinbasepro;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.coinbasepro.dto.CoinbaseProWebSocketTransaction;
import io.reactivex.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProProductBook;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProProductTicker;
import org.knowm.xchange.coinbasepro.dto.marketdata.CoinbaseProTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

import static io.netty.util.internal.StringUtil.isNullOrEmpty;
import static org.knowm.xchange.coinbasepro.CoinbaseProAdapters.*;

/**
 * Created by luca on 4/3/17.
 */
public class CoinbaseProStreamingMarketDataService implements StreamingMarketDataService {
    private static final Logger LOG = LoggerFactory.getLogger(CoinbaseProStreamingMarketDataService.class);

    private final CoinbaseProStreamingService service;
    private Map<CurrencyPair, SortedMap<BigDecimal, String>> bids = new HashMap<>();
    private Map<CurrencyPair, SortedMap<BigDecimal, String>> asks = new HashMap<>();

    CoinbaseProStreamingMarketDataService(CoinbaseProStreamingService service) {
        this.service = service;
    }

    private boolean containsPair(List<CurrencyPair> pairs, CurrencyPair pair) {
        for (CurrencyPair item : pairs) {
            if (item.compareTo(pair) == 0) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        if (!containsPair(service.getProduct().getOrderBook(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for orderbook", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        final int maxDepth = (args.length > 0 && args[0] instanceof Integer) ? (int) args[0] : 0;

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) &&
                        (message.getType().equals("snapshot") || message.getType().equals("l2update")) &&
                        message.getProductId().equals(channelName))
                .map(s -> {
                    if (s.getType().equals("snapshot")) {
                        bids.put(currencyPair, new TreeMap<>(java.util.Collections.reverseOrder()));
                        asks.put(currencyPair, new TreeMap<>());
                    }

                    CoinbaseProProductBook productBook = s.toCoinbaseProProductBook(bids.get(currencyPair), asks.get(currencyPair), maxDepth);
                    return adaptOrderBook(productBook, currencyPair);
                });
    }

    /**
     * Returns an Observable of {@link CoinbaseProProductTicker}, not converted to {@link Ticker}
     *
     * @param currencyPair the currency pair.
     * @param args         optional arguments.
     * @return an Observable of {@link CoinbaseProProductTicker}.
     */
    public Observable<CoinbaseProProductTicker> getRawTicker(CurrencyPair currencyPair, Object... args) {
        if (!containsPair(service.getProduct().getTicker(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for ticker", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) && message.getType().equals("match") &&
                        message.getProductId().equals(channelName))
                .map(CoinbaseProWebSocketTransaction::toCoinbaseProProductTicker);
    }

    /**
     * Returns the CoinbasePro ticker converted to the normalized XChange object.
     * CoinbasePro does not directly provide ticker data via web service.
     * As stated by: https://docs.gdax.com/#get-product-ticker, we can just listen for 'match' messages.
     *
     * @param currencyPair Currency pair of the ticker
     * @param args         optional parameters.
     * @return an Observable of normalized Ticker objects.
     */
    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        if (!containsPair(service.getProduct().getTicker(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for ticker", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) && message.getType().equals("ticker") &&
                        message.getProductId().equals(channelName))
                .map(s -> adaptTicker(s.toCoinbaseProProductTicker(), s.toCoinbaseProProductStats(), currencyPair));
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        if (!containsPair(service.getProduct().getTrades(), currencyPair))
            throw new UnsupportedOperationException(String.format("The currency pair %s is not subscribed for trades", currencyPair));

        String channelName = currencyPair.base.toString() + "-" + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Observable<CoinbaseProWebSocketTransaction> subscribedChannel = service.subscribeChannel(channelName)
                .map(s -> mapper.readValue(s.toString(), CoinbaseProWebSocketTransaction.class));

        return subscribedChannel
                .filter(message -> !isNullOrEmpty(message.getType()) && message.getType().equals("match") &&
                        message.getProductId().equals(channelName))
                .map(s -> {
                            Trades adaptedTrades = adaptTrades(new CoinbaseProTrade[]{s.toCoinbaseProTrade()}, currencyPair);
                            return adaptedTrades.getTrades().get(0);
                        }
                );
    }
}
