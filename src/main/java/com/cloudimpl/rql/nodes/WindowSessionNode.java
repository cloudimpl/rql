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
public class WindowSessionNode extends WindowNode{

    private JsonObject json;
    private long windowStart = 0;
    public WindowSessionNode(String interval, TimeUnit unit) {
        super(interval, unit);
    }

    @Override
    public Flux<Flux<JsonObject>> window(Flux<JsonObject> inputFlux) {
        return inputFlux.windowUntil(this::openWindow, true);
    }
    
    protected boolean openWindow(JsonObject input)
    {
        long current = System.currentTimeMillis();
        boolean open =  current - windowStart >= intervalToMillis(getInterval(),getUnit());
        if(open)
            windowStart = current;
        return open;
    }
}
