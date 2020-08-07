/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;

/**
 *
 * @author nuwansa
 */
public class FieldCheckNode implements RqlBoolNode{
    private final String fieldName;
    private final boolean checkExist;

    public FieldCheckNode(String fieldName, boolean checkExist) {
        this.fieldName = fieldName;
        this.checkExist = checkExist;
    }

    @Override
    public boolean eval(JsonObject val) {
        if(checkExist)
            return val.get(fieldName) != null && !val.get(fieldName).isJsonNull();
        else
            return val.get(fieldName) == null || val.get(fieldName).isJsonNull();
    }
    
}
