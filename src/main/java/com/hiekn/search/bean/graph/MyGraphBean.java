package com.hiekn.search.bean.graph;

import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.bean.graph.GraphStatBean;
import com.hiekn.plantdata.bean.graph.PathAGBean;
import com.hiekn.plantdata.bean.graph.RelationBean;

import java.util.List;

public class MyGraphBean {
    private List<MyEntityBean> entityList;
    private List<MyRelationBean> relationList;
    private List<PathAGBean> connects;
    private List<GraphStatBean> stats;
    private Integer level1HasNextPage;

    public List<MyEntityBean> getEntityList() {
        return entityList;
    }

    public void setEntityList(List<MyEntityBean> entityList) {
        this.entityList = entityList;
    }

    public List<MyRelationBean> getRelationList() {
        return relationList;
    }

    public void setRelationList(List<MyRelationBean> relationList) {
        this.relationList = relationList;
    }

    public List<PathAGBean> getConnects() {
        return connects;
    }
    public void setConnects(List<PathAGBean> connects) {
        this.connects = connects;
    }
    public List<GraphStatBean> getStats() {
        return stats;
    }
    public void setStats(List<GraphStatBean> stats) {
        this.stats = stats;
    }
    public Integer getLevel1HasNextPage() {
        return level1HasNextPage;
    }
    public void setLevel1HasNextPage(Integer level1HasNextPage) {
        this.level1HasNextPage = level1HasNextPage;
    }
}
