package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CexioData {

    private final String error;
    private final String ok;

    public CexioData(@JsonProperty("error") String error, @JsonProperty("ok") String ok) {
        this.error = error;
        this.ok = ok;
    }

    public String getError() {
        return error;
    }

    public String getOk() {
        return ok;
    }

    @Override
    public String toString() {
        return "CexioData{" +
                "error='" + error + '\'' +
                ", ok='" + ok + '\'' +
                '}';
    }

}
