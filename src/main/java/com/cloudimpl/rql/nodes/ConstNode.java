/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

/**
 *
 * @author nuwansa
 */
public class ConstNode implements RqlNode{
    private  String value;
    public ConstNode(String value) {
        this.value = value.trim();
        if(value.startsWith("'") || value.startsWith("\""))
            this.value = value.substring(1, value.length() - 1);
    }

    public String getValue() {
        return value;
    }
     
}
