/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.parser;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import static com.jayway.jsonpath.Criteria.where;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.Filter;
import static com.jayway.jsonpath.Filter.filter;
import com.jayway.jsonpath.JsonPath;
import static com.jayway.jsonpath.Option.DEFAULT_PATH_LEAF_TO_NULL;
import com.jayway.jsonpath.internal.path.PredicateContextImpl;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import java.util.HashMap;

/**
 *
 * @author nuwansa
 */
public class Test {

    public static void main(String[] args) {
        Gson gson = new Gson();
        String json = gson.toJson(new Student("nuwan", 30, new Address("abcs")));
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).mappingProvider(new GsonMappingProvider()).options(DEFAULT_PATH_LEAF_TO_NULL).build();
        DocumentContext document = JsonPath.using(conf).parse(json);
        Filter cheapFictionFilter = filter(
                where("name").eq("nuwan")
        );
        PredicateContextImpl pcxt = new PredicateContextImpl(document, document, conf, new HashMap<>());
        System.out.println(cheapFictionFilter.apply(pcxt));
        JsonObject map = document.read("$",cheapFictionFilter);
        System.out.println(map);
    }
}

class Student {

    private final String name;
    private final int age;
    private final Address addr;

    public Student(String name, int age, Address addr) {
        this.name = name;
        this.age = age;
        this.addr = addr;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

}

class Address {

    private String addr;

    public Address(String addr) {
        this.addr = addr;
    }

}
