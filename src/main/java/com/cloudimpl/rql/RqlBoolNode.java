/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.google.gson.JsonObject;

/**
 *
 * @author nuwansa
 */
public interface RqlBoolNode extends RqlNode{
     public boolean eval(JsonObject val);
}
