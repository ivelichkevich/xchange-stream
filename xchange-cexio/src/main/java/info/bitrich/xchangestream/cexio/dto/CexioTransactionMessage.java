package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CexioTransactionMessage {

    private final String e;
    private final CexioTransaction data;

    public CexioTransactionMessage(@JsonProperty("e") String e,
                                   @JsonProperty("data") CexioTransaction data) {
        this.e = e;
        this.data = data;
    }

    public String getE() {
        return e;
    }

    public CexioTransaction getData() {
        return data;
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer("{");
        buffer.append("e='").append(e).append('\'');
        buffer.append(", data=").append(data);
        buffer.append('}');
        return buffer.toString();
    }
}
