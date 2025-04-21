package com.wfa.crosstalk.core.model;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.wfa.middleware.utils.DataType;

public class HelperMethods {
	public static String getSchemaName(String topic) {
		return topic + "_ValueSchema";
	}
	
	public static String getRecordName(String topic) {
		return topic + "_Record";
	}
	
    public static DataType ApacheToApplicationDataType(Schema apacheField) {
    	Type apacheFieldType = apacheField.getType();
    	switch (apacheFieldType) {
    	case Type.UNION:
    		List<Schema> underlyingSchemas = apacheField.getTypes();
    		for (Schema schema: underlyingSchemas) {
    			if (!schema.getType().equals(Type.NULL))
    				return ApacheToApplicationDataType(schema);
    		}
    	case Type.INT:
    		return DataType.INTEGER;
    	case Type.DOUBLE:
    		return DataType.REAL;
    	default:
    		return DataType.STRING;
    	}
    }
    
    public static String ApplicationToKsqlDataType(DataType appDataType) {
    	switch(appDataType) {
    	case DataType.INTEGER:
    		return "BIGINT";
    	case DataType.REAL:
    		return "DOUBLE";
    	default:
    		return "STRING";
    	}
    }
    
    public static String ApacheDataTypeToKsqlDataType(Schema apacheField) {
    	return ApplicationToKsqlDataType(ApacheToApplicationDataType(apacheField));
    }
}
