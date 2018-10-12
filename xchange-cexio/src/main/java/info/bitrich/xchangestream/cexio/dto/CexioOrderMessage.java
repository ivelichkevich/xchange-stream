package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CexioOrderMessage {

    private final String e;
    private final CexioOrder data;

    public CexioOrderMessage(@JsonProperty("e") String e,
                             @JsonProperty("data") CexioOrder data) {
        this.e = e;
        this.data = data;
    }

    public String getE() {
        return e;
    }

    public CexioOrder getData() {
        return data;
    }

    @Override
    public String toString() {
        return "CexioOrderMessage {" +
                "e='" + e + '\'' +
                ", data=" + data +
                '}';
    }

}
