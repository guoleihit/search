package com.hiekn.search.bean.result;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PaperDetail extends PaperItem {


	private List<String> categories;
	private String citeCount;
	private String eTitle;

	/**
	 * 参考文献
	 */
	private List<Object> references;

    /**
     * 引证文献
     */
	private List<Object> cites;

    public List<Object> getCites() {
        return cites;
    }

    public void setCites(List<Object> cites) {
        this.cites = cites;
    }

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

	public List<Object> getReferences() {
		return references;
	}

	public void setReferences(List<Object> references) {
		this.references = references;
	}
}
