package com.hiekn.util;

import java.util.Properties;

public final class CommonResource {
	public static final String PROMPT_INDEX = "gw_prompt";
	public static final String STANDARD_INDEX = "gw_standard";
	public static final String PATENT_INDEX = "gw_patent";
	public static final String PAPER_INDEX = "gw_paper";
	public static final String BAIKE_INDEX = "gw_baike";
	public static final String PICTURE_INDEX = "gw_picture";
	public static final String RESULTS_INDEX = "gw_results";

	private static Properties props = PropertiesUtil.loadPropties("meta_ws.properties");

	public static final String swagger_base_path = props.getProperty("swagger_base_path");;

	public static final String swagger_ip_port = props.getProperty("swagger_ip_port");

	public static final String new_plantdata_service_url = props.getProperty("new_plantdata_service_url");

	public static final String kg_public_service_url = props.getProperty("kg_public_service_url");

	public static final Float search_person_weight = Float.valueOf(props.getProperty("search_person_weight"));
	public static final Float search_recognized_person_weight = Float.valueOf(props.getProperty("search_recognized_person_weight"));
	public static final Float search_org_weight = Float.valueOf(props.getProperty("search_org_weight"));
	public static final Float search_recognized_org_weight = Float.valueOf(props.getProperty("search_recognized_org_weight"));
	public static final Float search_patent_weight = Float.valueOf(props.getProperty("search_patent_weight"));
	public static final Float search_degree_paper_weight = Float.valueOf(props.getProperty("search_degree_paper_weight"));
	public static final Float search_conference_paper_weight = Float.valueOf(props.getProperty("search_conference_paper_weight"));
	public static final Float search_journal_paper_weight = Float.valueOf(props.getProperty("search_journal_paper_weight"));
    public static final Float search_user_input_title_weight = Float.valueOf(props.getProperty("search_user_input_title_weight"));
}