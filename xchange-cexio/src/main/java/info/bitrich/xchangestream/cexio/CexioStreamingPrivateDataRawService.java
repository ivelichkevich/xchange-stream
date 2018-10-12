package info.bitrich.xchangestream.cexio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.cexio.dto.CexioOrderMessage;
import info.bitrich.xchangestream.cexio.dto.CexioTransaction;
import info.bitrich.xchangestream.cexio.dto.CexioTransactionMessage;
import info.bitrich.xchangestream.core.StreamingPrivateDataService;
import io.reactivex.Observable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.dto.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CexioStreamingPrivateDataRawService implements StreamingPrivateDataService {

    private static final Logger LOG = LoggerFactory.getLogger(CexioStreamingPrivateDataRawService.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CexioStreamingService service;


    public CexioStreamingPrivateDataRawService(CexioStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<Order> getOrders() {
        return service.subscribeChannel(CexioStreamingService.ORDER)
                .map(it -> deserialize(it, CexioOrderMessage.class))
                .map(it -> CexioAdapters.adaptOrder(it.getData()))
                .doOnNext(it -> LOG.debug(String.format("Order is updated: %s", it)));
    }

    public Observable<CexioTransaction> getTransactions() {
        return service.subscribeChannel(CexioStreamingService.TRANSACTION)
                .map(it -> deserialize(it, CexioTransactionMessage.class))
                .map(CexioTransactionMessage::getData)
                .doOnNext(it -> LOG.debug(String.format("New transaction: %s", it)));
    }

    private <T> T deserialize(JsonNode message, Class<T> clazz) throws JsonProcessingException {
        return objectMapper.treeToValue(message, clazz);
    }
}
