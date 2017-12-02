package com.hiekn.search.bean.result;

public class NewsItem extends ItemBean{
    Long earliest_publication_date;
    String channel;
    String url;

    public Long getEarliest_publication_date() {
        return earliest_publication_date;
    }
    public void setEarliest_publication_date(Long earliest_publication_date) {
        this.earliest_publication_date = earliest_publication_date;
    }
    public String getChannel() {
        return channel;
    }
    public void setChannel(String channel) {
        this.channel = channel;
    }
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
}
