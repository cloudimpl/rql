/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public interface RqlEngine {

    void addEvent(Object event);
    Flux<Object> flux();
    
    public static RqlEngine create(String rql) {
        return new RqlEngineJsonImpl(rql).start();
    }
}
