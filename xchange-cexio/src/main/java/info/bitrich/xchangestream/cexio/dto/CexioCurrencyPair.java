package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CexioCurrencyPair {

    private final String symbol1;
    private final String symbol2;

    public CexioCurrencyPair(@JsonProperty("symbol1") String symbol1, @JsonProperty("symbol2") String symbol2) {
        this.symbol1 = symbol1;
        this.symbol2 = symbol2;
    }

    public String getSymbol1() {
        return symbol1;
    }

    public String getSymbol2() {
        return symbol2;
    }

    @Override
    public String toString() {
        return "CexioCurrencyPair{" +
                "symbol1='" + symbol1 + '\'' +
                ", symbol2='" + symbol2 + '\'' +
                '}';
    }

}
