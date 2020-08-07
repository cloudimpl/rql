/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public abstract class WindowNode implements RqlNode {

    private final TimeUnit unit;
    private final long interval;

    public WindowNode(String interval, TimeUnit unit) {
        this.unit = unit;
        this.interval = Long.valueOf(interval);
    }

    public long getInterval() {
        return interval;
    }

    protected long intervalToMillis(long interval,TimeUnit unit) {
        switch (unit) {
            case SECONDS:
                return interval * 1000;
            case MINUTES:
                return interval * 60 * 1000;
            case HOURS:
                return interval * 3600 * 1000;
            case DAYS:
                return interval * 24 * 3600 * 1000;
            default:
                return -1;
        }
    }

    public TimeUnit getUnit() {
        return unit;
    }

    
    public abstract Flux<Flux<JsonObject>> window(Flux<JsonObject> inputFlux);
}
