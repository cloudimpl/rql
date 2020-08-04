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
        Rql.from(Flux.interval(Duration.ofSeconds(1)).map(i->new Item(i)),"select i , name from stream where (i >= 10) and name is not null limit 10")           
                .doOnError(thr->thr.printStackTrace())
                .subscribe(System.out::println);
        
        Rql.from(Flux.interval(Duration.ofSeconds(1)).map(i->new Item(i)),"select name,school,sum(i) as total from stream window tumbling(size 10 seconds) group by name,school")             
                .doOnError(thr->thr.printStackTrace())
                .subscribe(System.out::println);
        Thread.sleep(10000000);
    }
}


class Item
{
    long i;
    String name;
    String school;
    public Item(long i) {
        this.i = i;
        this.name = Arrays.asList("nuwan","nuwan","abeysiriwardana").get(((int)i) % 3);
        this.school = Arrays.asList("Richmond","Richmond",null).get(((int)i) % 3);
    }

    @Override
    public String toString() {
        return "Item{" + "i=" + i + '}';
    }
    
    
}