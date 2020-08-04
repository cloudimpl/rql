/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public class ColumRefNode extends ColumnNode{
    private final String columnName;

    public ColumRefNode(String columnName) {
        this.columnName = columnName.trim();
        setAlias(this.columnName);
        
    }
    
    @Override
    public void eval(List<JsonObject> input,JsonObject output) {
        JsonElement el = input.get(0).get(columnName);
        if(el != null)
            output.add(getAlias(), el);
        else
            output.add(getAlias(), JsonNull.INSTANCE);
        
    }
    
}
