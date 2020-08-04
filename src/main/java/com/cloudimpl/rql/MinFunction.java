/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public class MinFunction extends AggregateColumnNode {

    private final String columnName;

    public MinFunction(String columnName) {
        this.columnName = columnName.trim();
    }


    @Override
    void  eval(List<JsonObject> input, JsonObject output) {
        int dataType = dataType(input);
        if(dataType == 1)
        {
            double d = input.stream().filter(json -> json.keySet().contains(columnName)).map(json -> json.get(columnName))
                .map(el->toVal(el)).mapToDouble(BigDecimal::doubleValue).min().getAsDouble();
            output.addProperty(getAlias(), d);
        }
        else
        {
            String max = input.stream().filter(json -> json.keySet().contains(columnName)).map(json -> json.get(columnName))
                    .map(el->el.getAsString()).min((l,r)->l.compareTo(r)).get();
             output.addProperty(getAlias(), max);
        }
        
    }

    private BigDecimal toVal(JsonElement el) {
        JsonPrimitive primitive = el.getAsJsonPrimitive();
        if (!primitive.isNumber()) {
            throw new RqlException("invalid data type for aggregation " + getAlias());
        }
        return primitive.getAsBigDecimal();
    }
    
    private int dataType(List<JsonObject> input)
    {
        JsonElement el = input.stream().filter(json->json.get(columnName) != null)
                .map(json->json.get(columnName))
                .findFirst().orElse(null);
        if(el == null)
            throw new RqlException("null value not support for aggregation");
        
        if(!el.isJsonPrimitive())
            throw new RqlException("only primitive value supported for aggregation");
        
        JsonPrimitive prim = el.getAsJsonPrimitive();
        if(prim.isNumber())
            return 1;
        else if(prim.isString())
            return 2;
        else
          throw new RqlException("value type not supported for aggregation");  
    }
}
