package com.hiekn.search.bean.result;

import java.util.List;

public class PaperDetail extends PaperItem {


	private List<String> categories;
	private String citeCount;
    public List<String> getCategories() {
		return categories;
	}

	public void setCategories(List<String> categories) {
		this.categories = categories;
	}

	public String getCiteCount() {
		return citeCount;
	}

	public void setCiteCount(String citeCount) {
		this.citeCount = citeCount;
	}
}
