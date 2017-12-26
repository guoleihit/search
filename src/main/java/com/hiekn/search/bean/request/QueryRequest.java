package com.hiekn.search.bean.request;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DefaultValue;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;

import io.swagger.annotations.ApiParam;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;

/**
 * 检索请求
 * @author leiguo
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class QueryRequest {

	@ApiParam(value="搜索词")
	@DefaultValue("")
	private String kw;

	@ApiParam(value="请求时间戳")
	private Long tt;

	@ApiParam(value="页码 默认")
	@DefaultValue("1") 
	private Integer pageNo = 1;

	@ApiParam(value="页数 默认20")
	@DefaultValue("20")
	private Integer pageSize = 20;

	@ApiParam(value="排序方式")
	private Integer sort;

	@ApiParam("查询信息来源,专利、论文、新闻...")
	private DocType docType;

	@ApiParam("关键词类别，作者=1、机构=2、关键词=3")
	private Integer kwType = 0;

	@ApiParam("过滤条件，比如 [{k: 'earliest_publication_date', v: ['2017'], d: '发表年份'}]")
	private List<KVBean<String,List<String>>> filters = new ArrayList<>();

    private String id;

    private String customQuery;

	/**
	 *  模糊 or 精确
	 *  <p>默认精确</p>
	 */
	private Integer precision = 1;

	public Integer getPrecision() {
		return precision;
	}

	public void setPrecision(Integer precision) {
		this.precision = precision;
	}

	public String getCustomQuery() {
        return customQuery;
    }

    public void setCustomQuery(String customQuery) {
        this.customQuery = customQuery;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    private List<DocType> docTypeList;


	/*
            private String andKwList;

            private String exactKwList;

            private String atLeastOneKw;

            private String noneKwList;

            @ApiParam("关键词出现位置，无限制=0，标题=1")
            @DefaultValue("0")
            private Integer position = 0;

            private String language;
        */
	private String description;

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<KVBean<String, List<String>>> getFilters() {
		return filters;
	}

	@JsonDeserialize(using=KVBeanListDeserializer.class)
	public void setFilters(List<KVBean<String, List<String>>> filters) {
		this.filters = filters;
	}

	public Integer getKwType() {
		return kwType;
	}

	public void setKwType(Integer kwType) {
		this.kwType = kwType;
	}

	public DocType getDocType() {
		return docType;
	}

	public void setDocType(DocType docType) {
		this.docType = docType;
	}

	public Integer getSort() {
		return sort;
	}

	public void setSort(Integer sort) {
		this.sort = sort;
	}

	public Integer getPageNo() {
		return pageNo;
	}

	public void setPageNo(Integer pageNo) {
		this.pageNo = pageNo;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public String getKw() {
		return kw;
	}

	public void setKw(String kw) {
		this.kw = kw;
	}

	public Long getTt() {
		return tt;
	}

	public void setTt(Long tt) {
		this.tt = tt;
	}


    public List<DocType> getDocTypeList() {
        return docTypeList;
    }

    public void setDocTypeList(List<DocType> docTypeList) {
        this.docTypeList = docTypeList;
    }
}
