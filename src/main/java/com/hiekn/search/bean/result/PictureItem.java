package com.hiekn.search.bean.result;

import java.util.List;

import com.hiekn.search.bean.DocType;

public class PictureItem extends ItemBean{

	private List<String> keywords;

	public PictureItem() {
		setDocType(DocType.PICTURE);
	}

	public void setKeywords(List<String> keywordList) {
		keywords = keywordList;
	}

	public List<String> getKeywords() {
		return keywords;
	}
}
