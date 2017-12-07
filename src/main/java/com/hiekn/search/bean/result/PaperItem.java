package com.hiekn.search.bean.result;

import java.util.List;
import java.util.Set;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

//@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PaperItem extends ItemBean {

	public PaperItem() {
		setDocType(DocType.PAPER);
	}

	private List<String> keywords;

	private String journal;

	private Long downloadedCount;

    /**
     * 参考文献
     */
    private List<String> references;

    private String firstAuthor;

    private String firstAuthorOrg;

    private Set<String> orgs;

    public String getFirstAuthor() {
        return firstAuthor;
    }

    public void setFirstAuthor(String firstAuthor) {
        this.firstAuthor = firstAuthor;
    }

    public String getFirstAuthorOrg() {
        return firstAuthorOrg;
    }

    public void setFirstAuthorOrg(String firstAuthorOrg) {
        this.firstAuthorOrg = firstAuthorOrg;
    }

    public Set<String> getOrgs() {
        return orgs;
    }

    public void setOrgs(Set<String> orgs) {
        this.orgs = orgs;
    }

    public List<String> getReferences() {
        return references;
    }

    public void setReferences(List<String> references) {
        this.references = references;
    }

	public Long getDownloadedCount() {
		return downloadedCount;
	}

	public void setDownloadedCount(Long downloadedCount) {
		this.downloadedCount = downloadedCount;
	}

	public String getJournal() {
        return journal;
    }

    public void setJournal(String journal) {
        this.journal = journal;
    }

	public List<String> getKeywords() {
		return keywords;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}
}
