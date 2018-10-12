package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import info.bitrich.xchangestream.cexio.CexioStreamingService;

public class CexioAuthMessage extends CexioAbstractCommandMessage {

    private final CexioAuthRequest auth;

    public CexioAuthMessage(@JsonProperty("auth") CexioAuthRequest auth) {
        super(CexioStreamingService.AUTH);
        this.auth = auth;
    }

    public CexioAuthRequest getAuth() {
        return auth;
    }

    @Override
    public String toString() {
        return "CexioAuthMessage{" +
                "e='" + getCommand() + '\'' +
                ", auth=" + auth +
                '}';
    }

}
