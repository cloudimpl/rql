/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class WindowTumblingNode extends WindowNode{

    public WindowTumblingNode(String interval,TimeUnit unit) {
        super(interval, unit);
    }

    @Override
    public Flux<Flux<JsonObject>> window(Flux<JsonObject> inputFlux) {
        return inputFlux.window(Duration.ofMillis(intervalToMillis(getInterval(),getUnit())));           
    }
   
    
    
    public static void main(String[] args) throws InterruptedException {
      //  Flux.interval(Duration.ofSeconds(1)).map(i->"evt"+i).window(Duration.ofSeconds(5),Duration.ofSeconds(2)).doOnNext(f->windowFlux(f)).subscribe();
        Flux.interval(Duration.ofSeconds(1))
                .doOnNext(l->System.out.println("l:"+l))
                .window(Duration.ofSeconds(10)).flatMap(f->f.groupBy(i->i % 2 == 0 ? "even":"odd").flatMap(flux->groupBy(flux)))
                .doOnNext(l->System.out.println("listXXXX:"+l))
                .doOnError(thr->thr.printStackTrace())
                .subscribe();
        Thread.sleep(1000000);
    }
    
    public static void windowFlux(Flux<String> windowFlux)
    {
        long prefix = System.currentTimeMillis();
        windowFlux.doOnSubscribe(s->System.out.println(prefix+":start"+System.currentTimeMillis())).doOnNext(e->System.out.println(prefix+":x:"+e))
                .doOnComplete(()->System.out.println(prefix+":end :"+System.currentTimeMillis())).subscribe();
        
    }
    
    public static Mono<List<Long>> groupBy(GroupedFlux<String,Long> flux)
    {
        return flux.collectList().doOnNext(list->System.out.println(flux.key()+":"+list));
    }
}
