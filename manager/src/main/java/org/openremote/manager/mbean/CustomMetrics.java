package org.openremote.manager.mbean;


import java.util.concurrent.atomic.AtomicLong;


public class CustomMetrics implements CustomMetricsMBean {

    private final AtomicLong totalFoo = new AtomicLong();

    @Override
    public long getTotalFoo() {
        return totalFoo.get();
    }

    public void incrementFoo() {
        totalFoo.incrementAndGet();
    }
}
