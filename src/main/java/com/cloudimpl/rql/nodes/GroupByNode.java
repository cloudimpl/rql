/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class GroupByNode implements RqlNode{
    Set<String> groupByFields = new LinkedHashSet<>();
    
    public GroupByNode addField(String field)
    {
        this.groupByFields.add(field.trim());
        return this;
    }
    
    public Flux<List<List<JsonObject>>> groupBy(Flux<Flux<JsonObject>> inputFlux)
    {
        return Flux.from(inputFlux.flatMap(flux->flux.groupBy(j->getKey(j)).flatMap(f->f.collectList()).collectList()));
    }
    
    private String getKey(JsonObject json)
    {
        return groupByFields.stream().map(f->json.get(f) == null?null:json.get(f).getAsString()).collect(Collectors.joining(":"));
    }
   
    
}
