package com.hiekn.search.bean.request;

import java.util.List;

public class StandardQueryRequest extends QueryRequest{

    private List<CompositeRequestItem> conditions;

    private Integer pubDateStart;

    private Integer pubDateEnd;

    public List<CompositeRequestItem> getConditions() {
        return conditions;
    }

    public void setConditions(List<CompositeRequestItem> conditions) {
        this.conditions = conditions;
    }

    public Integer getPubDateStart() {
        return pubDateStart;
    }

    public void setPubDateStart(Integer pubDateStart) {
        this.pubDateStart = pubDateStart;
    }

    public Integer getPubDateEnd() {
        return pubDateEnd;
    }

    public void setPubDateEnd(Integer pubDateEnd) {
        this.pubDateEnd = pubDateEnd;
    }

}
