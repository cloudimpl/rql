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
public class BinNode implements RqlBoolNode{
    
    public enum Op {AND , OR};
    
    private final RqlBoolNode left;
    private final Op op;
    private final RqlBoolNode right;

    public BinNode(RqlBoolNode left,Op op, RqlBoolNode right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    public RqlNode getLeft() {
        return left;
    }

    public Op getOp() {
        return op;
    }

    public RqlNode getRight() {
        return right;
    }

    @Override
    public boolean eval(JsonObject val) {
        switch(op)
        {
            case AND:
            {
               return this.left.eval(val) && this.right.eval(val);
            }
            case OR:
            {
               return this.left.eval(val) || this.right.eval(val);
            }
            default:
                return false;
        }
    }
    
    
    
}
