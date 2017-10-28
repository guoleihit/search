package com.hiekn.search.bean.result;

import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PaperItem extends ItemBean {

	public PaperItem() {
		setDocType(DocType.PAPER);
	}

	private List<String> keywords;

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}
}
