/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

/**
 *
 * @author nuwansa
 */
public class VarNode implements RqlNode{
    private final String var;

    public VarNode(String var) {
        this.var = var.trim();
    }

    public String getVar() {
        return var;
    }
    
    
}
