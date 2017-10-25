package com.hiekn.search.bean.result;

import java.util.ArrayList;
import java.util.List;

import com.hiekn.search.bean.DocType;

public class ItemBean {

	private String docId;
	private String title;
	private String abs;
	private List<String> authors;
	private List<String> agencies;
	private DocType docType;

	public ItemBean(){
		authors = new ArrayList<>();
		agencies = new ArrayList<>();
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

	public List<String> getAgencies() {
		return agencies;
	}

	public void setAgencies(List<String> agencies) {
		this.agencies = agencies;
	}

	public DocType getDocType() {
		return docType;
	}

	public void setDocType(DocType docType) {
		this.docType = docType;
	}
}
