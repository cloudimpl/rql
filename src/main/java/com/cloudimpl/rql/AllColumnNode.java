/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonObject;

/**
 *
 * @author nuwansa
 */
public class AllColumnNode extends ColumnNode{

    @Override
    Object eval(JsonObject input, JsonObject output) {
        input.entrySet().forEach(e->output.add(e.getKey(),e.getValue()));
        return input;
    }
    
}
