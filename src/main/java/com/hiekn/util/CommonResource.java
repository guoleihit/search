package com.hiekn.util;

import java.util.Properties;

public final class CommonResource {
	private static Properties props = PropertiesUtil.loadPropties("meta_ws.properties");

	public static final String swagger_base_path = props.getProperty("swagger_base_path");;

	public static final String swagger_ip_port = props.getProperty("swagger_ip_port");
	
	public static final String search_es_index = props.getProperty("search_es_index");
	public static final String search_es_type = props.getProperty("search_es_type");
	public static final String search_es_analyzer = props.getProperty("search_es_analyzer");
	
	public static final String search_kg_semantic_http_url = props.getProperty("search_kg_semantic_http_url");
	public static final String search_kg_name = props.getProperty("search_kg_name");
}