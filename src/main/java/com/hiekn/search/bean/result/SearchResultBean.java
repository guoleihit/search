package com.hiekn.search.bean.result;

import java.util.List;

import com.google.common.collect.Lists;

public class SearchResultBean {

	private String kw;
	private Long rsCount;
	private List<ItemBean> rsData;

	public SearchResultBean(String kw) {
		super();
		rsData = Lists.newArrayList();
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

}
