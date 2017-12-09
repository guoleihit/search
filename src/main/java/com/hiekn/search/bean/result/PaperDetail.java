package com.hiekn.search.bean.result;

import java.util.List;

public class PaperDetail extends PaperItem {


	private List<String> categories;
	private String citeCount;
	private String eTitle;

	public String geteTitle() {
		return eTitle;
	}

	public void seteTitle(String eTitle) {
		this.eTitle = eTitle;
	}

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
