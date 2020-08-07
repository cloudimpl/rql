/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;

/**
 *
 * @author nuwansa
 */
public class RelNode implements RqlBoolNode {

    public enum Op {
        EQ, GT, GTE, LT, LTE, NE
    }
    private final String fieldName;
    private final Op op;
    private final ConstNode constNode;

    public RelNode(String fieldName, Op op, ConstNode constNode) {
        this.fieldName = fieldName;
        this.op = op;
        this.constNode = constNode;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Op getOp() {
        return op;
    }

    public ConstNode getConstNode() {
        return constNode;
    }

    @Override
    public boolean eval(JsonObject val) {
        JsonElement el = val.get(fieldName);
        if (el == null) {
            return false;
        }
        if(el.isJsonPrimitive())
            return evalPrimitive(el.getAsJsonPrimitive(),constNode.getValue());
        return false;
    }

    private boolean evalPrimitive(JsonPrimitive el, String val) {
        if (el.isBoolean()) {
            if (op != Op.EQ) {
                throw new InvalidType("invalid operator type" + op + " for field : " + fieldName);
            }
            return el.getAsBoolean() == Boolean.valueOf(val);
        } else if (el.isNumber()) {
            BigDecimal left = el.getAsBigDecimal();
            BigDecimal right = new BigDecimal(val);
            switch (op) {
                case EQ:
                    return left.compareTo(right) == 0;
                case GT:
                    return left.compareTo(right) > 0;
                case GTE:
                    return left.compareTo(right) >= 0;
                case LT:
                    return left.compareTo(right) < 0;
                case LTE:
                    return left.compareTo(right) <= 0;
                case NE:
                    return left.compareTo(right) != 0;
                default:
                    return false;
            }
        } else if (el.isString()) {
            String left = el.getAsString();
            switch (op) {
                case EQ:
                    return left.equals(val);
                case GT:
                    return left.compareTo(val) > 0;
                case GTE:
                    return left.compareTo(val) >= 0;
                case LT:
                    return left.compareTo(val) < 0;
                case LTE:
                    return left.compareTo(val) <= 0;
                case NE:
                    return !left.equals(val);
                default:
                    return false;
            }
        }
        return false;
    }
}
