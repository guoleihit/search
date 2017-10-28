package com.hiekn.search.bean.request;

import java.util.List;

public class KGRequest {

	private String kw;
	
	private List<Long> allowAtts;
	
	private List<Long> allowTypes;

	public String getKw() {
		return kw;
	}

	public void setKw(String kw) {
		this.kw = kw;
	}

	public List<Long> getAllowAtts() {
		return allowAtts;
	}

	public void setAllowAtts(List<Long> allowAtts) {
		this.allowAtts = allowAtts;
	}

	public List<Long> getAllowTypes() {
		return allowTypes;
	}

	public void setAllowTypes(List<Long> allowTypes) {
		this.allowTypes = allowTypes;
	}
	
}
