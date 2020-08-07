/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public class ValueNode extends ColumnNode{

    private final String value;

    public ValueNode(String value) {
        this.value = value;
    }
    
    @Override
    public void eval(List<JsonObject> input,JsonObject output) {
        output.addProperty(getAlias(), value);
    }
    
}
