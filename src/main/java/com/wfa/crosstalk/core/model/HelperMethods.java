package com.wfa.crosstalk.core.model;

public class HelperMethods {
	public static String getSchemaName(String topic) {
		return topic + "_ValueSchema";
	}
	
	public static String getRecordName(String topic) {
		return topic + "_Record";
	}
}
