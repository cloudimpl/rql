/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 *
 * @author nuwansa
 */
public class FluxQueue {

    private final Flux<Object> flux;
    private final List<FluxSink<Object>> emitters = new CopyOnWriteArrayList<>();

    public FluxQueue() {
        flux = Flux.create(emitter -> {
            emitters.add(emitter);
            emitter.onCancel(() -> removeEmitter(emitter));
            emitter.onDispose(() -> removeEmitter(emitter));
        });
    }

    public Flux<Object> flux()
    {
        return flux;
    }
    
    public void add(Object event)
    {
        sinkNext(event);
    }
    
    public void error(Throwable thr)
    {
        emitters.forEach(emitter -> emitter.error(thr));
    }
    
    private void sinkNext(Object event) {
        emitters.forEach(emitter -> emitter.next(event));
    }

    private void removeEmitter(FluxSink sink) {
        emitters.remove(sink);
    }
}
