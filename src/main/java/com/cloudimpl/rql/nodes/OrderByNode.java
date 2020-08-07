/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author nuwansa
 */
public class OrderByNode implements RqlNode,Comparator<JsonObject>{
    private final Set<OrderByItem> fields = new LinkedHashSet<>();
    private Comparator<JsonObject> comparator;
    public OrderByNode addField(OrderByItem field)
    {
        this.fields.add(field);
        return this;
    }

    public void complete()
    {       
        Comparator comp = null;
        for(OrderByItem item : fields)
        {
            if(comp == null)
                comp = item.getComparator();
            else
                comp = comp.thenComparing(item.getComparator());
        }
        this.comparator = comp;
    }
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
        
        return this.comparator.compare(o1, o2);
    }
    
    public List<JsonObject> sort(List<JsonObject> input)
    {
         input.sort(this);
         return input;
    }
}
