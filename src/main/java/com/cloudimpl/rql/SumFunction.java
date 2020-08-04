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
public class SumFunction extends AggregateColumnNode {

    private final String columnName;

    public SumFunction(String columnName) {
        this.columnName = columnName.trim();
    }


    @Override
    void  eval(List<JsonObject> input, JsonObject output) {
        double d = input.stream().filter(json -> json.keySet().contains(columnName)).map(json -> json.get(columnName))
                .map(el->toVal(el)).mapToDouble(BigDecimal::doubleValue).sum();
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
