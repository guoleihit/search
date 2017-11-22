package com.hiekn.search.bean.result;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PatentItem extends ItemBean {

	private List<String> agencies;
	private List<String> applicants;
	private String applicationDate;
	private String applicationNumber;
	private String publicationNumber;
	private List<String> agents;
	private String mainIPC;

	private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public PatentItem() {
		setDocType(DocType.PATENT);
	}
	public List<String> getAgencies() {
		return agencies;
	}

	public void setAgencies(List<String> agencies) {
		this.agencies = agencies;
	}

	public List<String> getApplicants() {
		return applicants;
	}

	public void setApplicants(List<String> applicants) {
		this.applicants = applicants;
	}

	public String getApplicationDate() {
		return applicationDate;
	}

	public void setApplicationDate(String applicationDate) {
		this.applicationDate = applicationDate;
	}

    public String getApplicationNumber() {
        return applicationNumber;
    }

    public void setApplicationNumber(String applicationNumber) {
        this.applicationNumber = applicationNumber;
    }

    public String getPublicationNumber() {
        return publicationNumber;
    }

    public void setPublicationNumber(String publicationNumber) {
        this.publicationNumber = publicationNumber;
    }

    public List<String> getAgents() {
        return agents;
    }

    public void setAgents(List<String> agents) {
        this.agents = agents;
    }

    public String getMainIPC() {
        return mainIPC;
    }

    public void setMainIPC(String mainIPC) {
        this.mainIPC = mainIPC;
    }
}
