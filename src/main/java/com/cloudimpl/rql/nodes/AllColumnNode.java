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
public class AllColumnNode extends ColumnNode{

    @Override
    public void eval(List<JsonObject> input, JsonObject output) {
        input.get(0).entrySet().forEach(e->output.add(e.getKey(),e.getValue()));
    }
    
}
