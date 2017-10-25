package com.hiekn.search.factory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration  
public class FactoryBean {  
	
	@Value("${es_name}")  
	private String es_name;
	@Value("#{'${es_ip}'.split(',')}")
	private List<String> es_ip;
	@Value("${es_port}")
	private Integer es_port;
	
	
    @Bean
    public TransportClient esClient(){ 
    	TransportClient client = null;
    	if(es_name != null && !"".equals(es_name)){
			Settings settings = Settings.builder().put("cluster.name", es_name).build();
			client = new PreBuiltTransportClient(settings);
		}else{
			client = new PreBuiltTransportClient(Settings.EMPTY);
		}
		try {
			for(String ip : es_ip){
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), es_port));
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
        return client;  
    } 
 
}  
