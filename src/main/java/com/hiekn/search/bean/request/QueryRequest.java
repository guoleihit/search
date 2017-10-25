package com.hiekn.search.bean.request;

import javax.ws.rs.DefaultValue;

import com.hiekn.search.bean.DocType;

import io.swagger.annotations.ApiParam;

/**
 * 检索请求
 * @author leiguo
 *
 */
public class QueryRequest {

	@ApiParam(value="搜索词")
	@DefaultValue("")
	private String kw;

	@ApiParam(value="请求时间戳")
	private Long tt;

	@ApiParam(value="页码 默认")
	@DefaultValue("0") 
	private Integer pageNo = 0;

	@ApiParam(value="页数 默认20")
	@DefaultValue("20")
	private Integer pageSize = 20;

	@ApiParam(value="排序方式")
	private Integer sort;

	@ApiParam(value="查询类型，0=一框式 1=组合 2=句子")
	private Integer queryType;

	@ApiParam("查询信息来源,专利、论文、新闻...")
	private DocType docType;

	@ApiParam("关键词类别，作者=1、机构=2、关键词=3")
	private Integer kwType = 0;

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

	public Integer getQueryType() {
		return queryType;
	}

	public void setQueryType(Integer queryType) {
		this.queryType = queryType;
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
}
