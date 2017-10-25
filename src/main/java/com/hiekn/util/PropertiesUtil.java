package com.hiekn.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
	
	public static Properties loadPropties(String file){
		Properties props = new Properties();
		InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(file);
		try {
			props.load(in);
		} catch (IOException e) {
			System.exit(1);
		}
		return props;
	}
}
