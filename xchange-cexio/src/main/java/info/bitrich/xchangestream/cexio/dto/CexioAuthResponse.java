package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CexioAuthResponse {

    private final String e;
    private final CexioData data;
    private final String ok;
    private final long timestamp;

    public CexioAuthResponse(@JsonProperty("e") String e, @JsonProperty("data") CexioData data,
                             @JsonProperty("ok") String ok, @JsonProperty("timestamp") long timestamp) {
        this.e = e;
        this.data = data;
        this.ok = ok;
        this.timestamp = timestamp;
    }

    public String getE() {
        return e;
    }

    public CexioData getData() {
        return data;
    }

    public String getOk() {
        return ok;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isSuccess() {
        return getOk().equals("ok");
    }

    @Override
    public String toString() {
        return "CexioAuthResponse{" +
                "e='" + e + '\'' +
                ", data=" + data +
                ", ok='" + ok + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
