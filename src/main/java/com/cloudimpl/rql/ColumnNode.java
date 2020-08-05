/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonObject;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public abstract class ColumnNode implements RqlNode{
    private String alias;
    public ColumnNode() {
        alias = null;
    }

    public final ColumnNode setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    public String getAlias() {
        return alias;
    }
    
    
    public abstract void eval(List<JsonObject> input,JsonObject output);
}
