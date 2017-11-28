package com.hiekn.search.bean.result;

import com.hiekn.search.bean.DocType;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class BaikeItem extends ItemBean {

	private String eTitle;
	private String pyTitle;
	private List<String> contents;
	private List<Map<String, String>> pictures;

	public BaikeItem() {
		contents = new LinkedList<>();
		pictures = new LinkedList<>();
		setDocType(DocType.BAIKE);
	}

    public List<Map<String, String>> getPictures() {
        return pictures;
    }

    public void setPictures(List<Map<String, String>> pictures) {
        this.pictures = pictures;
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
