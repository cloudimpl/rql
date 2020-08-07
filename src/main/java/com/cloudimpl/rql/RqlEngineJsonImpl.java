/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.cloudimpl.rql.common.GsonCodec;
import com.cloudimpl.rql.nodes.SelectNode;
import com.cloudimpl.rql.parser.RqlParser;
import com.google.gson.JsonObject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class RqlEngineJsonImpl implements RqlEngine {

    private final FluxQueue eventSource;
    private final FluxQueue emitQueue;
    private final SelectNode selectNode;

    public RqlEngineJsonImpl(String rql) {
        eventSource = new FluxQueue();
        emitQueue = new FluxQueue();
        selectNode = RqlParser.parse(rql);
    }

    @Override
    public Flux<Object> flux() {
        return emitQueue.flux();
    }

    @Override
    public void addEvent(Object event) {
        eventSource.add(event);
    }

    public RqlEngine start()
    {
        Flux<JsonObject>  jsonFlux = eventSource.flux().map(o->GsonCodec.encodeToJson(o).getAsJsonObject());
        flux(jsonFlux).doOnNext(json->emitQueue.add(json)).doOnError(thr->emitQueue.error(thr)).subscribe();
        return this;
    }
    
    private Flux<JsonObject> flux(Flux<JsonObject> sourceFlux) {
        Flux<JsonObject> emitFlux = sourceFlux;
        if (selectNode.getExp() != null) {
            emitFlux = sourceFlux.filter(o -> selectNode.getExp().eval(o));
        }

        Flux<List<List<JsonObject>>> fluxList = null;
        if (selectNode.getWindowNode() != null) {
            Flux<Flux<JsonObject>> stream = selectNode.getWindowNode().window(emitFlux);
            if (selectNode.hasAggregates() && selectNode.getGroupBy() == null) {
                fluxList = stream.flatMap(f -> f.collectList().map(l -> Collections.singletonList(l)));
            } else if (selectNode.getGroupBy() != null) {
                fluxList = selectNode.getGroupBy().groupBy(stream);
            } else {
                fluxList = stream.flatMap(f -> f).map(j -> Collections.singletonList(Collections.singletonList(j)));
            }

        } else {
            fluxList = emitFlux.map(json -> Collections.singletonList(Collections.singletonList(json)));
        }

        emitFlux = fluxList.map(input -> expand(input)).map(list -> selectNode.getOrderBy() != null ? selectNode.getOrderBy().sort(list) : list).flatMapIterable(l -> l);
        if (selectNode.getLimit() > -1) {
            emitFlux = emitFlux.take(selectNode.getLimit());
        }
        return emitFlux;
    }
    
     public List<JsonObject> expand(List<List<JsonObject>> Input) {
        List<JsonObject> list = new LinkedList<>();
        for(List<JsonObject> l: Input)
        {
            JsonObject out = new JsonObject();
            selectNode.getColumnNodes().forEach(cn -> cn.eval(l, out));
            list.add(out);
        }      
        return list;
    }
}
