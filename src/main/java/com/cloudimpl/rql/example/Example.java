/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.example;

import com.cloudimpl.rql.RqlEngine;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class Example {

    public static void main(String[] args) throws InterruptedException {
        List<Integer> list = IntStream.range(1, 100).mapToObj(i -> i).collect(Collectors.toList());
        list.sort((l, r) -> Integer.compare(r, l));
        System.out.println("list:" + list);
//        
//        Rql.from(Flux.interval(Duration.ofSeconds(1)).map(i->new Item(i)),"select i , name from stream where (i >= 10) and name is not null limit 10")           
//                .doOnError(thr->thr.printStackTrace())
//                .subscribe(System.out::println);
//      
        RqlEngine engine = RqlEngine.create("select name,school,max(i) as max,sum(i) as total,min(i) as min ,count(*) as count  ,avg(i) as avg from stream window hopping(size 5 seconds,advance by 3 seconds) group by name,school order by name DESC,school asc");
        Flux.interval(Duration.ofSeconds(1)).map(i -> new Item(i)).doOnNext(evt -> engine.addEvent(evt)).subscribe();
        engine.flux().doOnError(thr -> thr.printStackTrace()).subscribe(System.out::println);
//        Rql.from(Flux.interval(Duration.ofSeconds(1))
//                // .map(i->new Item(i)),"select name,school,max(i) as max,sum(i) as total,min(i) as min ,count(*) as count  ,avg(i) as avg from stream window session(1 seconds) group by name,school order by name DESC,school asc")             
//                .map(i -> new Item(i)), "select name,school,max(i) as max,sum(i) as total,min(i) as min ,count(*) as count  ,avg(i) as avg from stream window hopping(size 5 seconds,advance by 3 seconds) group by name,school order by name DESC,school asc")
//                .doOnError(thr -> thr.printStackTrace())
//                .subscribe(System.out::println);
        Thread.sleep(10000000);
    }
}

class Item {

    long i;
    String name;
    String school;

    public Item(long i) {
        this.i = i;
        this.name = Arrays.asList("nuwan", "nuwan", "abeysiriwardana").get(((int) i) % 3);
        this.school = Arrays.asList("Richmond", "Royal", "Mahinda").get(((int) i) % 3);
    }

    @Override
    public String toString() {
        return "Item{" + "i=" + i + '}';
    }

}
