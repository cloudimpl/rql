/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.functions;

import com.cloudimpl.rql.nodes.AggregateColumnNode;
import com.cloudimpl.rql.nodes.RqlException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public class AvgFunction extends AggregateColumnNode{
    private final String columnName;

    public AvgFunction(String columnName) {
        this.columnName = columnName;
    }
    
    @Override
    public void eval(List<JsonObject> input, JsonObject output) {
         double d = input.stream().filter(json -> json.keySet().contains(columnName)).map(json -> json.get(columnName))
                .map(el->toVal(el)).mapToDouble(BigDecimal::doubleValue).average().getAsDouble();
        output.addProperty(getAlias(), d);
    }
    
    private BigDecimal toVal(JsonElement el) {
        JsonPrimitive primitive = el.getAsJsonPrimitive();
        if (!primitive.isNumber()) {
            throw new RqlException("invalid data type for aggregation " + getAlias());
        }
        return primitive.getAsBigDecimal();
    }
}
