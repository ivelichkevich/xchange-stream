package info.bitrich.xchangestream.coinbene;

import static io.reactivex.Observable.create;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
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
    private static final ScheduledExecutorService ses = newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CoinbeneRestScheduler").build());
    private static final Map<CurrencyPair, List<ObservableEmitter<OrderBook>>> emitersMap = new ConcurrentHashMap<>();
    private static final Map<CurrencyPair, String> orderBooksHashes = new ConcurrentHashMap<>();
    private static final Buffer fifo = BufferUtils.synchronizedBuffer(new CircularFifoBuffer());

    private static MarketDataService marketDataService;

    private final List<ObservableEmitter<OrderBook>> observableEmitters = new ArrayList<>();
    private boolean alive;

    static {
        Runnable task = () -> {
            if (fifo.isEmpty() || emitersMap.isEmpty()) {
                return;
            }
            CurrencyPair currencyPair = (CurrencyPair) fifo.remove();
            List<ObservableEmitter<OrderBook>> disposed = emitersMap.get(currencyPair).stream()
                    .filter(e -> e.isDisposed()).collect(toList());
            if (!disposed.isEmpty()) {
                LOG.info("\n Removing emitter for pair = " + currencyPair);
                disposed.forEach(e -> e.onComplete());
                emitersMap.get(currencyPair).removeAll(disposed);
                if (emitersMap.get(currencyPair).isEmpty()) {
                    return;
                }
            }

            try {
                final OrderBook orderBook = marketDataService.getOrderBook(currencyPair);
                String sha = getDigestShaOf(orderBook);
                if (orderBooksHashes.containsKey(currencyPair)) {
                    if (!orderBooksHashes.get(currencyPair).equals(sha)) {
                        orderBooksHashes.put(currencyPair, sha);
                        emitersMap.get(currencyPair).forEach(e -> e.onNext(orderBook));
                    }
                } else {
                    orderBooksHashes.put(currencyPair, sha);
                    emitersMap.get(currencyPair).forEach(e -> e.onNext(orderBook));
                }
            } catch (IOException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if (!fifo.contains(currencyPair)) {
                fifo.add(currencyPair);
            }
        };
        ses.scheduleAtFixedRate(task, 1000, 100, TimeUnit.MILLISECONDS);
    }

    CoinbeneStreamingMarketDataService(MarketDataService marketDataService) {
        if (CoinbeneStreamingMarketDataService.marketDataService == null) {
            CoinbeneStreamingMarketDataService.marketDataService = marketDataService;
        }
    }

    protected boolean isAlive() {
        return alive;
    }

    protected void disposeAll() {
        observableEmitters.forEach(e -> e.onComplete());
        observableEmitters.clear();
        alive = false;
    }

    /**
     * https://github.com/Coinbene/API-Documents/wiki/0.0.0-Coinbene-API-documents For every IP, except the "Place
     * order" and "Cancel order" API's access limit is 10times/10s,others is 100times/10s.
     *
     * @param currencyPair Currency pair of the order book
     */
    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        if (!fifo.contains(currencyPair)) {
            fifo.add(currencyPair);
        }

        Observable<OrderBook> observable = create(emitter -> {
            List<ObservableEmitter<OrderBook>> observableEmitters = emitersMap
                    .computeIfAbsent(currencyPair, k -> new ArrayList<>());
            observableEmitters.add(emitter);
            this.observableEmitters.add(emitter);
        });

        alive = true;
        return observable;
    }

    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        throw new NotAvailableFromExchangeException();
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        throw new NotAvailableFromExchangeException();
    }

    private static String getDigestShaOf(OrderBook orderBook) throws NoSuchAlgorithmException, IOException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        byte[] data = getOrderBookAsBytes(orderBook);
        byte[] digest = messageDigest.digest(data);
        return (new HexBinaryAdapter()).marshal(digest);
    }

    private static byte[] getOrderBookAsBytes(OrderBook orderBook) throws IOException {
        try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream)) {
            getLimitOrdersAsBytes(orderBook.getAsks(), objectOutputStream);
            getLimitOrdersAsBytes(orderBook.getBids(), objectOutputStream);
            return byteOutputStream.toByteArray();
        }
    }

    private static void getLimitOrdersAsBytes(List<LimitOrder> orders, ObjectOutputStream stream) throws IOException {
        Set<LimitOrder> ordersTree = new TreeSet<>(Comparator.comparing(LimitOrder::getLimitPrice));
        ordersTree.addAll(orders);
        for (LimitOrder ask : ordersTree) {
            //stream.writeObject(ask);
            stream.writeObject(ask.getLimitPrice());
            stream.writeObject(ask.getOriginalAmount());
        }
    }
}
