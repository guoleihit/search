package com.hiekn.search.bean.result;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.hiekn.search.bean.KVBean;

public class SearchResultBean {

	private String kw;
	private Long rsCount;
	private List<ItemBean> rsData;
	private List<KVBean<String,Map<String,? extends Object>>> filters;

	public SearchResultBean(String kw) {
		super();
		rsData = Lists.newArrayList();
		filters = Lists.newArrayList();
		this.kw = kw;
	}

	public String getKw() {
		return kw;
	}

	public void setKw(String kw) {
		this.kw = kw;
	}

	public Long getRsCount() {
		return rsCount;
	}

	public void setRsCount(Long rsCount) {
		this.rsCount = rsCount;
	}

	public List<ItemBean> getRsData() {
		return rsData;
	}

	public void setRsData(List<ItemBean> rsData) {
		this.rsData = rsData;
	}

	public List<KVBean<String, Map<String, ? extends Object>>> getFilters() {
		return filters;
	}

	public void setFilters(List<KVBean<String, Map<String, ? extends Object>>> filters) {
		this.filters = filters;
	}
}
