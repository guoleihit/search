package com.hiekn.search.bean.result.paper;

import com.hiekn.search.bean.result.PaperDetail;

public class Journal extends PaperDetail {

    /**
     * 期刊英文名字
     */
    private String eJournal;

    /**
     * 期刊年
     * <p>2002</p>
     */
    private String journalYear;

    /**
     * 卷
     */
    private String period;

    public Journal() {
        super();
        setPaperType(PaperType.JOURNAL);
    }

    public String geteJournal() {
        return eJournal;
    }

    public void seteJournal(String eJournal) {
        this.eJournal = eJournal;
    }

    public String getJournalYear() {
        return journalYear;
    }

    public void setJournalYear(String journalYear) {
        this.journalYear = journalYear;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }
}
