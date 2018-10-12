package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import info.bitrich.xchangestream.cexio.CexioStreamingService;

import java.util.Collections;
import java.util.List;

public class CexioSubscriptionMessage extends CexioAbstractCommandMessage {

    @JsonProperty("rooms")
    private final List<String> rooms;

    public CexioSubscriptionMessage(String pair) {
        super(CexioStreamingService.SUBSCRIBE);
        this.rooms = Collections.singletonList(pair);
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer("{");
        buffer.append("rooms=").append(rooms);
        buffer.append(", command='").append(command).append('\'');
        buffer.append('}');
        return buffer.toString();
    }
}
