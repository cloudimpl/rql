/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql;

import com.cloudimpl.rql.common.GsonCodec;
import com.cloudimpl.rql.parser.RqlParser;
import com.google.gson.JsonObject;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class Rql {
    public static Flux<JsonObject> from(Flux source,String rql)
    {
        SelectNode select = RqlParser.parse(rql);
        Flux<JsonObject> src = source.map(o->GsonCodec.encodeToJson(o).getAsJsonObject());
        return select.flux(src);
    }
}