package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

abstract public class CexioAbstractCommandMessage {

    @JsonProperty("e")
    protected final String command;

    public CexioAbstractCommandMessage(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
