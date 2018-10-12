package info.bitrich.xchangestream.service.netty.strategy;

import java.time.Duration;

public class DefaultHeartbeatStrategy implements HeartbeatStrategy {

    @Override
    public Duration getPeriod() {
        return Duration.ofSeconds(20);
    }
}
