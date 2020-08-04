/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author nuwansa
 */
public class OrderByNode implements RqlNode{
    private final Set<String> fields = new HashSet<>();
    
    public OrderByNode addField(String field)
    {
        this.fields.add(field.trim());
        return this;
    }
}
