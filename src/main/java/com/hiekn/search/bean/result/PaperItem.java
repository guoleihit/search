package com.hiekn.search.bean.result;

import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.result.paper.PaperType;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PaperItem extends ItemBean {

	public PaperItem() {
		setDocType(DocType.PAPER);
	}

	private List<String> keywords;

	private String journal;

	private Long downloadedCount;

    private String firstAuthor;

    private List<String> firstAuthorOrg;

    private Set<String> orgs;

    private PaperType paperType;

    private String origin;

    private String url;

    private Map<String, String> urls;

    private String doi;

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Map<String, String> getUrls() {
        return urls;
    }

    public void setUrls(Map<String, String> urls) {
        this.urls = urls;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public PaperType getPaperType() {
        return paperType;
    }

    public void setPaperType(PaperType paperType) {
        this.paperType = paperType;
    }

    public String getFirstAuthor() {
        return firstAuthor;
    }

    public void setFirstAuthor(String firstAuthor) {
        this.firstAuthor = firstAuthor;
    }

    public List<String> getFirstAuthorOrg() {
        return firstAuthorOrg;
    }

    public void setFirstAuthorOrg(List<String> firstAuthorOrg) {
        this.firstAuthorOrg = firstAuthorOrg;
    }

    public Set<String> getOrgs() {
        return orgs;
    }

    public void setOrgs(Set<String> orgs) {
        this.orgs = orgs;
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
