/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonObject;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public abstract class WindowNode implements RqlNode{
    
    public abstract Flux<Flux<JsonObject>> window(Flux<JsonObject> inputFlux);
}
