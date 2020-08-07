/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import com.google.gson.JsonObject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class SelectNode implements RqlNode {

    private List<ColumnNode> columnNodes = new LinkedList<>();
    private String tableName;
    private WindowNode windowNode;
    private RqlBoolNode exp;
    private GroupByNode groupBy;
    private OrderByNode orderBy;
    private int colIndex = 0;
    private long limit = -1;

    public SelectNode setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public SelectNode setExpression(RqlBoolNode exp) {
        this.exp = exp;
        return this;
    }

    public SelectNode setWindowNode(WindowNode window) {
        this.windowNode = window;
        return this;
    }

    public SelectNode setGroupBy(GroupByNode groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    public SelectNode setOrderBy(OrderByNode orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    public RqlBoolNode getExp() {
        return exp;
    }

    public GroupByNode getGroupBy() {
        return groupBy;
    }

    public long getLimit() {
        return limit;
    }

    public OrderByNode getOrderBy() {
        return orderBy;
    }

    public WindowNode getWindowNode() {
        return windowNode;
    }

    public List<ColumnNode> getColumnNodes() {
        return columnNodes;
    }

    
    public Flux<JsonObject> flux(Flux<JsonObject> sourceFlux) {
        Flux<JsonObject> emitFlux = sourceFlux;
        if (exp != null) {
            emitFlux = sourceFlux.filter(o -> this.exp.eval(o));
        }

        Flux<List<List<JsonObject>>> fluxList = null;
        if (windowNode != null) {
            Flux<Flux<JsonObject>> stream = windowNode.window(emitFlux);
            if (hasAggregates() && groupBy == null) {
                fluxList = stream.flatMap(f -> f.collectList().map(l->Collections.singletonList(l)));
            } else if (groupBy != null) {
                fluxList = groupBy.groupBy(stream);
            } else {
                fluxList = stream.flatMap(f -> f).map(j -> Collections.singletonList(Collections.singletonList(j)));
            }

        } else {
            fluxList = emitFlux.map(json -> Collections.singletonList(Collections.singletonList(json)));
        }

        emitFlux = fluxList.map(input -> expand(input)).map(list->orderBy != null?orderBy.sort(list):list).flatMapIterable(l->l);
        if (limit > -1) {
            emitFlux = emitFlux.take(limit);
        }
        return emitFlux;
    }

    public SelectNode complete() {
        List<ColumnNode> nonAggrColumns = columnNodes.stream().filter(n -> !(n instanceof AggregateColumnNode)).collect(Collectors.toList());
        List<String> colList = nonAggrColumns.stream().map(n -> n.getAlias()).collect(Collectors.toList());

        if (hasAggregates()) {
            if (!colList.isEmpty() && groupBy == null) {
                throw new RqlException("columns " + colList + " should on the group by list");
            } else if (!colList.isEmpty() && !groupBy.groupByFields.containsAll(colList)) {
                throw new RqlException("columns " + colList.removeAll(groupBy.groupByFields) + " should on the group by list");
            }
        } else if (groupBy != null) {
            throw new RqlException("columns " + colList.removeAll(groupBy.groupByFields) + " should on the group by list");
        }

        if(orderBy != null)
            orderBy.complete();
        return this;
    }

    public boolean hasAggregates() {
        return this.columnNodes.stream().filter(n -> n instanceof AggregateColumnNode).findAny().orElse(null) != null;
    }

    public SelectNode addColumn(ColumnNode node) {
        if (node.getAlias() == null) {
            node.setAlias("col-" + colIndex++);
        }
        columnNodes.add(node);
        return this;
    }

    public SelectNode setLimit(String limit) {
        this.limit = Long.valueOf(limit);
        return this;
    }

    public List<JsonObject> expand(List<List<JsonObject>> Input) {
        List<JsonObject> list = new LinkedList<>();
        for(List<JsonObject> l: Input)
        {
            JsonObject out = new JsonObject();
            columnNodes.forEach(cn -> cn.eval(l, out));
            list.add(out);
        }      
        return list;
    }

    @Override
    public String toString() {
        return "SelectNode{" + "columnNodes=" + columnNodes + ", tableName=" + tableName + ", windowNode=" + windowNode + ", exp=" + exp + ", groupBy=" + groupBy + ", orderBy=" + orderBy + ", colIndex=" + colIndex + ", limit=" + limit + '}';
    }
    
    

}
