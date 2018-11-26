package info.bitrich.xchangestream.coinbene;

import static io.reactivex.Observable.create;
import static java.util.concurrent.CompletableFuture.runAsync;

import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.exceptions.NotAvailableFromExchangeException;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoinbeneStreamingMarketDataService implements StreamingMarketDataService {

    private static final Logger LOG = LoggerFactory.getLogger(CoinbeneStreamingMarketDataService.class);
    private final MarketDataService marketDataService;
    private final Map<CurrencyPair, String> orderBooksHashes = new ConcurrentHashMap<>();

    CoinbeneStreamingMarketDataService(MarketDataService marketDataService) {
        this.marketDataService = marketDataService;
    }

    /**
     * https://github.com/Coinbene/API-Documents/wiki/0.0.0-Coinbene-API-documents
     * For every IP, except the "Place order" and "Cancel order" API's access limit is 10times/10s,others is 100times/10s.
     *
     * @param currencyPair Currency pair of the order book
     * @param args
     * @return
     */
    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        return create(emitter -> runAsync(() -> {
            while (true) {
                try {
                    OrderBook orderBook = marketDataService.getOrderBook(currencyPair, args);
                    String sha = getDigestShaOf(orderBook);
                    if (orderBooksHashes.containsKey(currencyPair)) {
                        if (!orderBooksHashes.get(currencyPair).equals(sha)) {
                            orderBooksHashes.put(currencyPair, sha);
                            emitter.onNext(orderBook);
                        }
                    } else {
                        orderBooksHashes.put(currencyPair, sha);
                        emitter.onNext(orderBook);
                    }
                } catch (IOException e) {
                    LOG.error("Error on getting OrderBook", e);
                } catch (NoSuchAlgorithmException e) {
                    LOG.error("Error on SAH algorithm", e);
                }
                try {
                    Thread.sleep(200); //TODO replace for one timer for all instances
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }));
    }

    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        throw new NotAvailableFromExchangeException();
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        throw new NotAvailableFromExchangeException();
    }

    private String getDigestShaOf(OrderBook orderBook) throws NoSuchAlgorithmException, IOException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        byte[] data = getOrderBookAsBytes(orderBook);
        byte[] digest = messageDigest.digest(data);
        return (new HexBinaryAdapter()).marshal(digest);
    }

    private byte[] getOrderBookAsBytes(OrderBook orderBook) throws IOException {
        try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream)) {
            getLimitOrdersAsBytes(orderBook.getAsks(), objectOutputStream);
            getLimitOrdersAsBytes(orderBook.getBids(), objectOutputStream);
            return byteOutputStream.toByteArray();
        }
    }

    private void getLimitOrdersAsBytes(List<LimitOrder> orders, ObjectOutputStream stream) throws IOException {
        Set<LimitOrder> ordersTree = new TreeSet<>(Comparator.comparing(LimitOrder::getLimitPrice));
        ordersTree.addAll(orders);
        for (LimitOrder ask : ordersTree) {
            //stream.writeObject(ask);
            stream.writeObject(ask.getLimitPrice());
            stream.writeObject(ask.getOriginalAmount());
        }
    }
}
