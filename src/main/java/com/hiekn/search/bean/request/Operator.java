package com.hiekn.search.bean.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Operator{

    AND("AND"), OR("OR"), NOT("NOT");
    private final String name;

    @JsonCreator
    Operator(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
