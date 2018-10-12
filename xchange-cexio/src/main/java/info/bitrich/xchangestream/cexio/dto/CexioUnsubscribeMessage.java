package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import info.bitrich.xchangestream.cexio.CexioStreamingService;

import java.util.Collections;
import java.util.List;

public class CexioUnsubscribeMessage extends CexioAbstractCommandMessage {

    @JsonProperty("rooms")
    private final List<String> rooms;

    public CexioUnsubscribeMessage(String pair) {
        super(CexioStreamingService.UNSUBSCRIBE);
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
