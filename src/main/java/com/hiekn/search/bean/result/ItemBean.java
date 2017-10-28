package com.hiekn.search.bean.result;

import java.util.ArrayList;
import java.util.List;

import com.hiekn.search.bean.DocType;

public class ItemBean {

	private String docId;
	private String title;
	private String abs;
	private List<String> authors;
	private String pubDate;
	private DocType docType;

	public String getPubDate() {
		return pubDate;
	}

	public void setPubDate(String pubDate) {
		this.pubDate = pubDate;
	}

	public ItemBean(){
		authors = new ArrayList<>();
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAbs() {
		return abs;
	}

	public void setAbs(String abs) {
		this.abs = abs;
	}

	public List<String> getAuthors() {
		return authors;
	}

	public void setAuthors(List<String> authors) {
		this.authors = authors;
	}

	public DocType getDocType() {
		return docType;
	}

	public void setDocType(DocType docType) {
		this.docType = docType;
	}
}
