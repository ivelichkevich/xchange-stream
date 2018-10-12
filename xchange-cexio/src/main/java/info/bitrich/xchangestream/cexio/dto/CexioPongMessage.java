package info.bitrich.xchangestream.cexio.dto;

import info.bitrich.xchangestream.cexio.CexioStreamingService;

public class CexioPongMessage extends CexioAbstractCommandMessage {

    public CexioPongMessage() {
        super(CexioStreamingService.PONG);
    }

    @Override
    public String toString() {
        return "CexioPongMessage{" +
                "e='" + getCommand() + '\'' +
                '}';
    }

}
