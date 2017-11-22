package com.hiekn.search.bean.request;

import com.hiekn.search.bean.KVBean;
import java.util.List;
import java.util.Map;

public class CompositeRequestItem {
    private Operator op;

    private KVBean<String, List<String>> kv;

    private KVBean<String, Map<String, Integer>> kvDate;

    /**
     * 1=精确，2=模糊
     */
    private Integer precision;

    public KVBean<String, Map<String, Integer>> getKvDate() {
        return kvDate;
    }

    public void setKvDate(KVBean<String, Map<String, Integer>> kvDate) {
        this.kvDate = kvDate;
    }

    public Operator getOp() {
        return op;
    }

    public void setOp(Operator op) {
        this.op = op;
    }

    public KVBean<String, List<String>> getKv() {
        return kv;
    }

    public void setKv(KVBean<String, List<String>> kv) {
        this.kv = kv;
    }

    /**
     * 1=精确，2=模糊
     */
    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }
}
