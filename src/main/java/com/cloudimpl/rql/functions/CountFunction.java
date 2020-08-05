/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.functions;

import com.cloudimpl.rql.AggregateColumnNode;
import com.google.gson.JsonObject;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public class CountFunction extends AggregateColumnNode{
    private final String columnName;

    public CountFunction(String columnName) {
        this.columnName = columnName.trim();
    }
    
    @Override
    public void eval(List<JsonObject> input, JsonObject output) {
        if(this.columnName.equals("*"))
        {
            output.addProperty(getAlias(),input.size());
        }
        else
        {
            long count = input.stream().filter(json->json.get(columnName) != null).count();
            output.addProperty(getAlias(), count);
        }
    }
    
}
