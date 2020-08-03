/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.example;

import com.cloudimpl.rql.Rql;
import java.time.Duration;
import java.util.Arrays;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class Example {
     
    public static void main(String[] args) throws InterruptedException {
        Rql.from(Flux.interval(Duration.ofSeconds(1)).map(i->new Item(i)),"select i , name from stream where (i >= 10) and name is not null")
                .doOnError(thr->thr.printStackTrace())
                .subscribe(System.out::println);
        Thread.sleep(10000000);
    }
}


class Item
{
    long i;
    String name;
    public Item(long i) {
        this.i = i;
        this.name = Arrays.asList("nuwan","sanjeewa","abeysiriwardana").get(((int)i) % 3);
    }

    @Override
    public String toString() {
        return "Item{" + "i=" + i + '}';
    }
    
    
}