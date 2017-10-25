package com.hiekn.util;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

public class JSONUtils {
	
	private static ThreadLocal<Gson> local = new ThreadLocal<Gson>();
	
	public static Gson getGson() {
		if (local.get() == null) {
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
				public java.util.Date deserialize(JsonElement p1, Type p2,JsonDeserializationContext p3) {
					return new java.util.Date(p1.getAsLong());
				}
			}).registerTypeAdapter(java.util.Date.class, new JsonSerializer<Date>(){
				public JsonElement serialize(Date arg0, Type arg1,JsonSerializationContext arg2) {
					return new JsonPrimitive(arg0.getTime());
				}
			});
			Gson gson = gsonBuilder.create();
			local.set(gson);
			return gson;
		} else {
			return local.get();
		}
	}

	public static <T> T fromJson(String json, Class<T> cls) {
		return getGson().fromJson(json, cls);
	}
	
	public static <T> T fromJson(String json,  Type typeOfT) {
		return getGson().fromJson(json, typeOfT);
	}
	
	public static String toJson(Object obj) {
		return getGson().toJson(obj);
	}
	
	public static <T> List<T> jsonToList(String json) {
		Type type = new TypeToken<List<T>>(){}.getType();
		return getGson().fromJson(json, type);
	}
	
	public static <T> List<T> fromJsonArray(String json, Class<T> clazz) throws Exception {
		List<T> lst = new ArrayList<T>();
		JsonArray array = new JsonParser().parse(json).getAsJsonArray();
		for(final JsonElement elem : array){
			lst.add(getGson().fromJson(elem, clazz));
		}
		return lst;
	} 
	
}