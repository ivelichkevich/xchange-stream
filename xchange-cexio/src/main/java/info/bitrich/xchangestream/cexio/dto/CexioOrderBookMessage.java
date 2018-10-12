package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import info.bitrich.xchangestream.cexio.CexioStreamingService;

import java.math.BigDecimal;
import java.util.List;

public class CexioOrderBookMessage extends CexioAbstractCommandMessage {

    private final CexioOrderBookData data;

    public static class CexioOrderBookData {

        private final Long id;
        private final String pair;
        private final Long buy_total;
        private final Long sell_total;
        private final List<PriceAmountPair> sell;
        private final List<PriceAmountPair> buy;

        @JsonFormat(shape = JsonFormat.Shape.ARRAY)
        public static class PriceAmountPair {
            private final BigDecimal price;
            private final BigDecimal amount;

            public PriceAmountPair(@JsonProperty("price") BigDecimal price, @JsonProperty("amount") BigDecimal amount) {
                this.price = price;
                this.amount = amount;
            }

            public PriceAmountPair(@JsonProperty("price") Double price, @JsonProperty("amount") Long amount) {
                this.price = BigDecimal.valueOf(price);
                this.amount = BigDecimal.valueOf(amount);
            }

            public BigDecimal getPrice() {
                return price;
            }

            public BigDecimal getAmount() {
                return amount;
            }

            @Override
            public String toString() {
                final StringBuffer buffer = new StringBuffer("{");
                buffer.append("price=").append(price);
                buffer.append(", amount=").append(amount);
                buffer.append('}');
                return buffer.toString();
            }
        }

        public CexioOrderBookData(@JsonProperty("id") Long id,
                                  @JsonProperty("pair") String pair,
                                  @JsonProperty("buy_total") Long buy_total,
                                  @JsonProperty("sell_total") Long sell_total,
                                  @JsonProperty("sell") List<PriceAmountPair> sell,
                                  @JsonProperty("buy") List<PriceAmountPair> buy) {
            this.id = id;
            this.pair = pair;
            this.buy_total = buy_total;
            this.sell_total = sell_total;
            this.sell = sell;
            this.buy = buy;
        }

        public String getPair() {
            return pair;
        }

        public List<PriceAmountPair> getSell() {
            return sell;
        }

        public List<PriceAmountPair> getBuy() {
            return buy;
        }

        @Override
        public String toString() {
            final StringBuffer buffer = new StringBuffer("{");
            buffer.append("id=").append(id);
            buffer.append(", pair='").append(pair).append('\'');
            buffer.append(", buy_total=").append(buy_total);
            buffer.append(", sell_total=").append(sell_total);
            buffer.append(", sell=").append(sell);
            buffer.append(", buy=").append(buy);
            buffer.append('}');
            return buffer.toString();
        }
    }

    public CexioOrderBookMessage(@JsonProperty("data") CexioOrderBookData data) {
        super(CexioStreamingService.MARKET_DEPTH);
        this.data = data;
    }

    public CexioOrderBookData getData() {
        return data;
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer("{");
        buffer.append("data=").append(data);
        buffer.append(", command='").append(command).append('\'');
        buffer.append('}');
        return buffer.toString();
    }
}
