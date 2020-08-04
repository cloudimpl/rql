/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonObject;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

/**
 *
 * @author nuwansa
 */
public class GroupByNode implements RqlNode{
    Set<String> groupByFields = new HashSet<>();
    
    public GroupByNode addField(String field)
    {
        this.groupByFields.add(field.trim());
        return this;
    }
    
    public Flux<GroupedFlux<String,JsonObject>> groupBy(Flux<Flux<JsonObject>> inputFlux)
    {
        return inputFlux.flatMap(flux->flux.groupBy(j->getKey(j)));
    }
    
    private String getKey(JsonObject json)
    {
        return groupByFields.stream().map(f->json.get(f) == null?null:json.get(f).getAsString()).collect(Collectors.joining(":"));
    }
}
