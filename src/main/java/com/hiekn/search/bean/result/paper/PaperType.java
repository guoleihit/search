package com.hiekn.search.bean.result.paper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum PaperType {
    JOURNAL("JOURNAL"), CONFERENCE("CONFERENCE"), DEGREE("DEGREE");

    private final String name;

    @JsonCreator
    PaperType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
