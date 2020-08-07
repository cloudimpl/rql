/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.rql.nodes;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author nuwansa
 */
public class TimeUnitNode implements RqlNode {

    TimeUnit unit;

    public TimeUnitNode(String unit) {
        this.unit = convert(unit);
    }

    private TimeUnit convert(String unit) {
        switch (unit) {
            case "seconds":
            case "second":
                return TimeUnit.SECONDS;
            case "minutes":
            case "minute":
                return TimeUnit.MINUTES;
            case "hours":
            case "hour":
                return TimeUnit.HOURS;
            case "days":
            case "day":
                return TimeUnit.DAYS;
            default:
                return null;

        }
    }

    public TimeUnit getTimeUnit()
    {
        return this.unit;
    }
}
