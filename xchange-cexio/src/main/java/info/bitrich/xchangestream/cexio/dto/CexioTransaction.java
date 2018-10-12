package info.bitrich.xchangestream.cexio.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.Date;

public class CexioTransaction {

    private final String id;
    private final String d;
    private final String c;
    private final BigDecimal a;
    private final BigDecimal ds;
    private final BigDecimal cs;
    private final String user;
    private final String symbol;
    private final String symbol2;
    private final BigDecimal amount;
    private final Long buy;
    private final Long order;
    private final Long sell;
    private final BigDecimal price;
    private final String type;
    private final Date time;
    private final BigDecimal balance;
    private final BigDecimal feeAmount;

    private final String txId;
    private final Long confirmations;
    private final String status;
    private final String address;

    public CexioTransaction(@JsonProperty("id") String id,
                            @JsonProperty("d") String d,
                            @JsonProperty("c") String c,
                            @JsonProperty("a") BigDecimal a,
                            @JsonProperty("ds") BigDecimal ds,
                            @JsonProperty("cs") BigDecimal cs,
                            @JsonProperty("user") String user,
                            @JsonProperty("symbol") String symbol,
                            @JsonProperty("symbol2") String symbol2,
                            @JsonProperty("amount") BigDecimal amount,
                            @JsonProperty("order") Long order,
                            @JsonProperty("buy") Long buy,
                            @JsonProperty("sell") Long sell,
                            @JsonProperty("price") BigDecimal price,
                            @JsonProperty("type") String type,
                            @JsonProperty("time") Date time,
                            @JsonProperty("balance") BigDecimal balance,
                            @JsonProperty("fee_amount") BigDecimal feeAmount,

                            // Balance operation specific data
                            @JsonProperty("txid") String txId,
                            @JsonProperty("confirmations") Long confirmations,
                            @JsonProperty("status") String status,
                            @JsonProperty("address") String address) {
        this.id = id;
        this.d = d;
        this.c = c;
        this.a = a;
        this.ds = ds;
        this.cs = cs;
        this.user = user;
        this.symbol = symbol;
        this.symbol2 = symbol2;
        this.amount = amount;
        this.buy = buy;
        this.order = order;
        this.sell = sell;
        this.price = price;
        this.type = type;
        this.time = time;
        this.balance = balance;
        this.feeAmount = feeAmount;

        this.txId = txId;
        this.confirmations = confirmations;
        this.status = status;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public String getD() {
        return d;
    }

    public String getC() {
        return c;
    }

    public BigDecimal getA() {
        return a;
    }

    public BigDecimal getDs() {
        return ds;
    }

    public BigDecimal getCs() {
        return cs;
    }

    public String getUser() {
        return user;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getSymbol2() {
        return symbol2;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public Long getBuy() {
        return buy;
    }

    public Long getOrder() {
        return order;
    }

    public Long getSell() {
        return sell;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public String getType() {
        return type;
    }

    public Date getTime() {
        return time;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public BigDecimal getFeeAmount() {
        return feeAmount;
    }

    public String getTxId() {
        return txId;
    }

    public Long getConfirmations() {
        return confirmations;
    }

    public String getStatus() {
        return status;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CexioTransaction)) return false;

        CexioTransaction that = (CexioTransaction) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (d != null ? !d.equals(that.d) : that.d != null) return false;
        if (c != null ? !c.equals(that.c) : that.c != null) return false;
        if (a != null ? !a.equals(that.a) : that.a != null) return false;
        if (ds != null ? !ds.equals(that.ds) : that.ds != null) return false;
        if (cs != null ? !cs.equals(that.cs) : that.cs != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;
        if (symbol != null ? !symbol.equals(that.symbol) : that.symbol != null) return false;
        if (symbol2 != null ? !symbol2.equals(that.symbol2) : that.symbol2 != null) return false;
        if (amount != null ? !amount.equals(that.amount) : that.amount != null) return false;
        if (buy != null ? !buy.equals(that.buy) : that.buy != null) return false;
        if (order != null ? !order.equals(that.order) : that.order != null) return false;
        if (sell != null ? !sell.equals(that.sell) : that.sell != null) return false;
        if (price != null ? !price.equals(that.price) : that.price != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (time != null ? !time.equals(that.time) : that.time != null) return false;
        if (balance != null ? !balance.equals(that.balance) : that.balance != null) return false;
        if (feeAmount != null ? !feeAmount.equals(that.feeAmount) : that.feeAmount != null) return false;
        if (txId != null ? !txId.equals(that.txId) : that.txId != null) return false;
        if (confirmations != null ? !confirmations.equals(that.confirmations) : that.confirmations != null)
            return false;
        if (status != null ? !status.equals(that.status) : that.status != null) return false;
        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (d != null ? d.hashCode() : 0);
        result = 31 * result + (c != null ? c.hashCode() : 0);
        result = 31 * result + (a != null ? a.hashCode() : 0);
        result = 31 * result + (ds != null ? ds.hashCode() : 0);
        result = 31 * result + (cs != null ? cs.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (symbol != null ? symbol.hashCode() : 0);
        result = 31 * result + (symbol2 != null ? symbol2.hashCode() : 0);
        result = 31 * result + (amount != null ? amount.hashCode() : 0);
        result = 31 * result + (buy != null ? buy.hashCode() : 0);
        result = 31 * result + (order != null ? order.hashCode() : 0);
        result = 31 * result + (sell != null ? sell.hashCode() : 0);
        result = 31 * result + (price != null ? price.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (time != null ? time.hashCode() : 0);
        result = 31 * result + (balance != null ? balance.hashCode() : 0);
        result = 31 * result + (feeAmount != null ? feeAmount.hashCode() : 0);
        result = 31 * result + (txId != null ? txId.hashCode() : 0);
        result = 31 * result + (confirmations != null ? confirmations.hashCode() : 0);
        result = 31 * result + (status != null ? status.hashCode() : 0);
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuffer buffer = new StringBuffer("{");
        buffer.append("id='").append(id).append('\'');
        buffer.append(", d='").append(d).append('\'');
        buffer.append(", c='").append(c).append('\'');
        buffer.append(", a=").append(a);
        buffer.append(", ds=").append(ds);
        buffer.append(", cs=").append(cs);
        buffer.append(", user='").append(user).append('\'');
        buffer.append(", symbol='").append(symbol).append('\'');
        buffer.append(", symbol2='").append(symbol2).append('\'');
        buffer.append(", amount=").append(amount);
        buffer.append(", buy=").append(buy);
        buffer.append(", order=").append(order);
        buffer.append(", sell=").append(sell);
        buffer.append(", price=").append(price);
        buffer.append(", type='").append(type).append('\'');
        buffer.append(", time=").append(time);
        buffer.append(", balance=").append(balance);
        buffer.append(", feeAmount=").append(feeAmount);
        buffer.append(", txId='").append(txId).append('\'');
        buffer.append(", confirmations=").append(confirmations);
        buffer.append(", status='").append(status).append('\'');
        buffer.append(", address='").append(address).append('\'');
        buffer.append('}');
        return buffer.toString();
    }
}
