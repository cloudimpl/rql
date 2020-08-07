/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Comparator;
import java.util.Objects;

/**
 *
 * @author nuwansa
 */
public class OrderByItem implements RqlNode{

    public enum OrderBy {
        ASC, DESC
    }
    private final String fieldName;
    private OrderBy orderBy;

    private final Comparator<JsonObject> comparator;
    public OrderByItem(String fieldName, String orderBy) {
        this.fieldName = fieldName.trim();
        this.orderBy = OrderBy.valueOf(orderBy.trim().toUpperCase());
        this.comparator = this::compare;
    }

    public OrderByItem setOrderBy(String orderBy) {
        if (!orderBy.isEmpty()) {
            this.orderBy = OrderBy.valueOf(orderBy.trim().toUpperCase());
        }
        return this;
    }

    public Comparator<JsonObject> getComparator()
    {
        return orderBy == OrderBy.DESC? comparator.reversed():comparator;
    }

    private int compare(JsonObject o1, JsonObject o2) {
        JsonElement left = o1.get(fieldName);
        JsonElement right = o2.get(fieldName);
        
        if(left == null && right == null)
            return 0;
        if(left == null)
            return -1;
        if(right == null)
            return 1;
        if(!left.isJsonPrimitive()  || !right.isJsonPrimitive())
            throw new RqlException("order by only support primitive values.error on field :"+fieldName);
        JsonPrimitive leftPrim = left.getAsJsonPrimitive();
        JsonPrimitive rightPrim = right.getAsJsonPrimitive();
        
        if(leftPrim.isString() || rightPrim.isString())
        {
            return leftPrim.getAsString().compareTo(right.getAsString());
        }
        else if(leftPrim.isNumber())
        {
            return leftPrim.getAsBigDecimal().compareTo(rightPrim.getAsBigDecimal());
        }
        throw new RqlException("order by only support primitive values(String | Number).error on field :"+fieldName);    
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.fieldName);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final OrderByItem other = (OrderByItem) obj;
        if (!Objects.equals(this.fieldName, other.fieldName)) {
            return false;
        }
        return true;
    }
}
