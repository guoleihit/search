package com.hiekn.search.bean.result.paper;

import com.hiekn.search.bean.result.PaperDetail;

public class Conference extends PaperDetail {

    /**
     * 会议名称
     */
    private String conference;
    private String conferenceDate;
    private String conferencePlace;
    /**
     * 主办方
     */
    private String conferenceOrganizer;
    public Conference(){
        super();
        setPaperType(PaperType.CONFERENCE);
    }


    public String getConference() {
        return conference;
    }

    public void setConference(String conference) {
        this.conference = conference;
    }

    public String getConferenceDate() {
        return conferenceDate;
    }

    public void setConferenceDate(String conferenceDate) {
        this.conferenceDate = conferenceDate;
    }

    public String getConferencePlace() {
        return conferencePlace;
    }

    public void setConferencePlace(String conferencePlace) {
        this.conferencePlace = conferencePlace;
    }

    public String getConferenceOrganizer() {
        return conferenceOrganizer;
    }

    public void setConferenceOrganizer(String conferenceOrganizer) {
        this.conferenceOrganizer = conferenceOrganizer;
    }
}
