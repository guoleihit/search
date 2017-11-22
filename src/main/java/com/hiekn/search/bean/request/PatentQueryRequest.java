package com.hiekn.search.bean.request;

import java.util.List;

public class PatentQueryRequest extends  QueryRequest{

    private List<CompositeRequestItem> conditions;

    private String appNum;
    private String pubNum;
    private String title;
    private String applicant;
    private String abs;
    private String author;
    private String ipc;
    private String keyword;

    /**
     * 0=全部,1=发明专利,2=实用新型,3=外观专利
     */
    private Integer type = 0;

    private Integer appDateStart;
    private Integer appDateEnd;
    private Integer pubDateStart;
    private Integer pubDateEnd;

    public List<CompositeRequestItem> getConditions() {
        return conditions;
    }

    public void setConditions(List<CompositeRequestItem> conditions) {
        this.conditions = conditions;
    }

    public String getAppNum() {
        return appNum;
    }

    public void setAppNum(String appNum) {
        this.appNum = appNum;
    }

    public String getPubNum() {
        return pubNum;
    }

    public void setPubNum(String pubNum) {
        this.pubNum = pubNum;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getApplicant() {
        return applicant;
    }

    public void setApplicant(String applicant) {
        this.applicant = applicant;
    }

    public String getAbs() {
        return abs;
    }

    public void setAbs(String abs) {
        this.abs = abs;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getIpc() {
        return ipc;
    }

    public void setIpc(String ipc) {
        this.ipc = ipc;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getAppDateStart() {
        return appDateStart;
    }

    public void setAppDateStart(Integer appDateStart) {
        this.appDateStart = appDateStart;
    }

    public Integer getAppDateEnd() {
        return appDateEnd;
    }

    public void setAppDateEnd(Integer appDateEnd) {
        this.appDateEnd = appDateEnd;
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
