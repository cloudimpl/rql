/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonObject;
import java.util.LinkedList;
import java.util.List;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class SelectNode implements RqlNode{
    private List<Selector> selectors = new LinkedList<>();
    private List<ColumnNode> columnNodes = new LinkedList<>();
    private String tableName;
    private RqlBoolNode exp;
    private int colIndex = 0;
    private long limit = -1;
    public SelectNode setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public SelectNode setExpression(RqlBoolNode exp)
    {
        this.exp = exp;
        return this;
    }
    
    public Flux<JsonObject> flux(Flux<JsonObject> sourceFlux)
    {
        Flux<JsonObject> emitFlux = sourceFlux;
        if(exp != null)
            emitFlux = sourceFlux.filter(o->this.exp.eval(o));
                                
        emitFlux = emitFlux.map(input->expand(input));
        if(limit > -1)
            emitFlux = emitFlux.take(limit);
        return emitFlux;
    }
    
    public SelectNode addColumn(ColumnNode node)
    {
        if(node.getAlias() == null)
            node.setAlias("col-"+colIndex++);
        columnNodes.add(node);
        return this;
    }
    
    public SelectNode setLimit(String limit)
    {
        this.limit = Long.valueOf(limit);
        return this;
    }
    public JsonObject expand(JsonObject Input)
    {
        JsonObject out = new JsonObject();
        columnNodes.forEach(cn->cn.eval(Input, out));
        return out;
    }
    
    @Override
    public String toString() {
        return "ParserNode{" + "selectors=" + selectors + ", tableName=" + tableName + '}';
    }
    
    
}
