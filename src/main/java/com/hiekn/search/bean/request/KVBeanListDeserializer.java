package com.hiekn.search.bean.request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.gson.reflect.TypeToken;
import com.hiekn.search.bean.KVBean;
import com.hiekn.util.JSONUtils;

public class KVBeanListDeserializer extends JsonDeserializer<List<KVBean<String,List<String>>>>{

	@Override
	public List<KVBean<String,List<String>>> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        String str = jp.getValueAsString();
        System.out.println("@@"+str);
        if (StringUtils.isEmpty(str)) {
        		return new ArrayList<>();
        }
		return JSONUtils.fromJson(str, new TypeToken<List<KVBean<String,List<String>>>>(){}.getType());
	}

}
