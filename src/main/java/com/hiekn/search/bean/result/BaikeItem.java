package com.hiekn.search.bean.result;

import java.util.LinkedList;
import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class BaikeItem extends ItemBean {

	private String eTitle;
	private String pyTitle;
	private List<String> contents;

	public BaikeItem() {
		contents = new LinkedList<>();
		setDocType(DocType.BAIKE);
	}

	public String geteTitle() {
		return eTitle;
	}

	public void seteTitle(String eTitle) {
		this.eTitle = eTitle;
	}

	public String getPyTitle() {
		return pyTitle;
	}

	public void setPyTitle(String pyTitle) {
		this.pyTitle = pyTitle;
	}

	public List<String> getContents() {
		return contents;
	}

	public void setContents(List<String> contents) {
		this.contents = contents;
	}
}
