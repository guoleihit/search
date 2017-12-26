package com.hiekn.search.bean.request;

import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;

import java.util.List;

public class QueryRequestInternal extends QueryRequest {
    private QueryRequest instance;

    public QueryRequestInternal(QueryRequest request){
        instance = request;
    }
    private String recognizedPerson;
    private String recognizedOrg;

    private List<AnalyzeResponse.AnalyzeToken> segmentList;

    public List<String> getUserSplitSegList() {
        return userSplitSegList;
    }

    public void setUserSplitSegList(List<String> userSplitSegList) {
        this.userSplitSegList = userSplitSegList;
    }

    private List<String> userSplitSegList;

    public List<AnalyzeResponse.AnalyzeToken> getSegmentList() {
        return segmentList;
    }

    public void setSegmentList(List<AnalyzeResponse.AnalyzeToken> segmentList) {
        this.segmentList = segmentList;
    }
    public String getRecognizedPerson() {
        return recognizedPerson;
    }

    public void setRecognizedPerson(String recognizedPerson) {
        this.recognizedPerson = recognizedPerson;
    }

    public String getRecognizedOrg() {
        return recognizedOrg;
    }

    public void setRecognizedOrg(String recognizedOrg) {
        this.recognizedOrg = recognizedOrg;
    }

    @Override
    public Integer getPrecision() {
        return instance.getPrecision();
    }

    @Override
    public void setPrecision(Integer precision) {
        instance.setPrecision(precision);
    }

    @Override
    public String getCustomQuery() {
        return instance.getCustomQuery();
    }

    @Override
    public void setCustomQuery(String customQuery) {
        instance.setCustomQuery(customQuery);
    }

    @Override
    public String getId() {
        return instance.getId();
    }

    @Override
    public void setId(String id) {
        instance.setId(id);
    }

    @Override
    public String getDescription() {
        return instance.getDescription();
    }

    @Override
    public void setDescription(String description) {
        instance.setDescription(description);
    }

    @Override
    public List<KVBean<String, List<String>>> getFilters() {
        return instance.getFilters();
    }

    @Override
    public void setFilters(List<KVBean<String, List<String>>> filters) {
        instance.setFilters(filters);
    }

    @Override
    public Integer getKwType() {
        return instance.getKwType();
    }

    @Override
    public void setKwType(Integer kwType) {
        instance.setKwType(kwType);
    }

    @Override
    public DocType getDocType() {
        return instance.getDocType();
    }

    @Override
    public void setDocType(DocType docType) {
        instance.setDocType(docType);
    }

    @Override
    public Integer getSort() {
        return instance.getSort();
    }

    @Override
    public void setSort(Integer sort) {
        instance.setSort(sort);
    }

    @Override
    public Integer getPageNo() {
        return instance.getPageNo();
    }

    @Override
    public void setPageNo(Integer pageNo) {
        instance.setPageNo(pageNo);
    }

    @Override
    public Integer getPageSize() {
        return instance.getPageSize();
    }

    @Override
    public void setPageSize(Integer pageSize) {
        instance.setPageSize(pageSize);
    }

    @Override
    public String getKw() {
        return instance.getKw();
    }

    @Override
    public void setKw(String kw) {
        instance.setKw(kw);
    }

    @Override
    public Long getTt() {
        return instance.getTt();
    }

    @Override
    public void setTt(Long tt) {
        instance.setTt(tt);
    }

    @Override
    public List<DocType> getDocTypeList() {
        return instance.getDocTypeList();
    }

    @Override
    public void setDocTypeList(List<DocType> docTypeList) {
        instance.setDocTypeList(docTypeList);
    }
}
