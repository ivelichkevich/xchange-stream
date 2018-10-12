package info.bitrich.xchangestream.cexio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.cexio.dto.CexioOrderBookMessage;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;

import java.io.IOException;

public class CexioStreamingMarketDataService implements StreamingMarketDataService {

    private final CexioStreamingService service;
    private final ObjectMapper mapper = new ObjectMapper();

    CexioStreamingMarketDataService(CexioStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        return getCexioOrderBook(currencyPair).map(CexioAdapters::adaptOrderBook);
    }

    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        return getCexioOrderBook(currencyPair).map(CexioAdapters::adaptOrderBookToTicker);
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

    private CexioOrderBookMessage deserialize(JsonNode message, TypeReference<CexioOrderBookMessage> type)
            throws IOException {
        return mapper.readValue(message.toString(), type);
    }

    private Observable<CexioOrderBookMessage> getCexioOrderBook(CurrencyPair currencyPair) {
        String room = String.format("pair-%s-%s", currencyPair.base, currencyPair.counter);
        return service.subscribeChannel(CexioStreamingService.MARKET_DEPTH, room)
                .map(it -> deserialize(it, new TypeReference<CexioOrderBookMessage>() {}));
    }
}
