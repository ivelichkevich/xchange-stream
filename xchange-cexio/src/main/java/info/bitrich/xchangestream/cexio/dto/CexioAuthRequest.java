package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CexioAuthRequest {

    private final String key;
    private final String signature;
    private final long timestamp;

    public CexioAuthRequest(@JsonProperty("key") String key,
                            @JsonProperty("signature") String signature,
                            @JsonProperty("timestamp") long timestamp) {
        this.key = key;
        this.signature = signature;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public String getSignature() {
        return signature;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "CexioAuthRequest{" +
                "key='" + key + '\'' +
                ", signature='" + signature + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}
