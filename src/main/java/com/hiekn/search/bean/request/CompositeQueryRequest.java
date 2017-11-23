package com.hiekn.search.bean.request;

import java.util.List;

public class CompositeQueryRequest extends QueryRequest{
    private List<CompositeRequestItem> conditions;

    public List<CompositeRequestItem> getConditions() {
        return conditions;
    }

    public void setConditions(List<CompositeRequestItem> conditions) {
        this.conditions = conditions;
    }
}
