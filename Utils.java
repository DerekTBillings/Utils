package com.billings.utils;

import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.billings.wrappers.Encoder;

public class Utils {
	
	private static Set<Integer> invalidCharCodes;
	
	public static Method[] getMethodsForAttributes(Class type, String... attributes) throws Exception {
		return findMethodsForAttributes(type, "get", attributes);
	}
	
	
	public static Method[] setMethodsForAttributes(Class type, String... attributes) throws Exception {
		return findMethodsForAttributes(type, "set", attributes);
	}
	
	
	private static Method[] findMethodsForAttributes(Class type, String accessType, String... attributes) throws Exception {
    	int attributeCount = attributes.length;
    	int accessTypeLength = accessType.length();

    	Method[] associatedMethods = new Method[attributeCount];
    	
    	Method[] allMethodsInObject = type.getMethods();
    	
    	Map<String, Method> methodMap = new HashMap<String, Method>();
    	
    	for (Method method : allMethodsInObject) {
    		String methodName = method.getName();
    		
    		if (methodName.length() > accessTypeLength && methodName.startsWith(accessType)) {
    			String trimmedName = methodName.substring(accessTypeLength);
    			String standardizedName = standardize(trimmedName);
    			
    			methodMap.put(standardizedName, method);
    		}
    	}
    	
    	for (int i=0; i<attributeCount; i++) {
    		String attribute = attributes[i];
    		String standardizedAttribute = standardize(attribute);
    		
    		Method associatedMethod = methodMap.get(standardizedAttribute);
    		
    		if (associatedMethod == null) {
    			String verbiage = String.format("No method found for attribute name %s in class %s", attribute, type);
    			throw new Exception(verbiage);
    		}
    		
    		associatedMethods[i] = associatedMethod;
    	}
    	
    	return associatedMethods;
    }
	
	
	private static String standardize(String str) {
		str = str.replaceAll("[^A-Za-z0-9]", "");
		str = str.toLowerCase();
		
		return str;
	}
	
	
	public static void printBeanAsJSON(Object bean, HttpServletResponse response, String... attributes) {
		try {
			String json = createJSON(bean, attributes);
			printJSON(json, response);
			
		} catch(Exception e) {
			printJSONException(e);
		}
	}
	
	
	public static void printMapAsJSON(Map map, HttpServletResponse response) {
		try {
			String json = createJSON(map);
			printJSON(json, response);
		
		} catch(Exception e) {
			printJSONException(e);
		}
	}
	
	
	private static void printJSONException(Exception e) {
		System.out.println("Couldn't create JSON from provided parameters");
		e.printStackTrace();
	}
	
	
	public static void printListAsJSON(List dataList, HttpServletResponse response, String... attributes) {
		try {
			String json = createJSONArray(dataList, attributes);
			printJSON(json, response);
			
		} catch(Exception e) {
			printJSONException(e);
		}
	}
	
	
	public static void printListAsJSON(List<Map<String, Object>> dataList, HttpServletResponse response) {
		try {
			String json = createJSONArray(dataList);
			printJSON(json, response);
			
		} catch(Exception e) {
			printJSONException(e);
		}
	}
	
	
	public static String createJSON(Object data, String... attributes) throws Exception {
		Class type = data.getClass();
		
		Method[] methods = getMethodsForAttributes(type, attributes);
		Object[] values = getValueArray(data, methods);
		
		StringBuffer json = createJSON(attributes, values);
		
		return json.toString();
	}
	
	
	private static Object[] getValueArray(Object data, Method[] methods) throws Exception {
		int valueSize = methods.length;
		
		Object[] values = new Object[valueSize];
		
		for (int i=0; i<valueSize; i++) {
			values[i] = methods[i].invoke(data);
		}
		
		return values;
	}
	
	
	public static String createJSON(Map<String, Object> map) throws Exception {
		String[] keys = getMapKeys(map);

		Object[] values = getValueArray(map, keys);
		
		StringBuffer json = createJSON(keys, values);
		
		return json.toString();
	}
	
	
	private static String[] getMapKeys(Map<String, Object> map) {
		Set<String> keySet = map.keySet();
		
		String[] keys = (String[])keySet.toArray();
		
		return keys;
	}
	
	
	private static Object[] getValueArray(Map<String, Object> data, Object[] keys) throws Exception {
		int valueSize = keys.length;
		
		Object[] values = new Object[valueSize];
		
		for (int i=0; i<valueSize; i++) {
			values[i] = data.get(keys[i]);
		}
		
		return values;
	}
	
	
	public static String createJSONArray(List<Map<String, Object>> dataList) throws Exception {
		if (dataList == null || dataList.size() == 0) {
			return null;
		} 
		
		Map<String, Object> firstMap = dataList.get(0);
		
		String[] keys = getMapKeys(firstMap);
		
		StringBuffer jsonArray = new StringBuffer("[");
		
		for (int i=0; i<dataList.size(); i++) {
			if (i > 0) {
				jsonArray.append(",");
			} 
			
			Map data = dataList.get(i);
			
			Object[] values = getValueArray(data, keys);
			
			StringBuffer json = createJSON(keys, values);
			
			jsonArray.append(json);
		}
		
		jsonArray.append("]");
		
		return jsonArray.toString();
	}
	
	
	public static String createJSONArray(List dataList, String... attributes) throws Exception {
		if (dataList == null || dataList.size() == 0) {
			return null;
		}
		
		StringBuffer jsonArray = new StringBuffer("[");
		
		Class type = dataList.get(0).getClass();
		
		Method[] methods = getMethodsForAttributes(type, attributes);
		
		for (int i=0; i<dataList.size(); i++) {
			if (i > 0) {
				jsonArray.append(",");
			} 

			Object data = dataList.get(i);
			
			Object[] values = getValueArray(data, methods);
			
			StringBuffer json = createJSON(attributes, values);
			
			jsonArray.append(json);
		}
		
		jsonArray.append("]");
		
		return jsonArray.toString();
	}
	
	
	private static StringBuffer createJSON(String[] keys, Object[] values) throws Exception {
		StringBuffer json = new StringBuffer("{");
		
		for (int i=0; i<keys.length; i++) {
			if (i != 0) {
				json.append(", ");
			}
			
			String key = keys[i];
			Object objValue = values[i];
			
			String value = (String) cast(objValue, String.class);
			
			value = sanitizeString(value);
			
			//replaces all instances of '\' with '\\'
			//extra slashes are needed to provide proper output
			value = value.replaceAll("\\\\", "\\\\\\\\");
			
			//replaces instances of '"' with '\"'
			value = value.replaceAll("\"", "\\\\\"");
			
			String input = String.format("\"%s\": \"%s\"", key, value);
			
			json.append(input);
		}
		
		json.append("}");
		
		
		return json;
	}
	
	
	public static Object cast(Object target, Class castType) {
		if (target == null || target.getClass() == castType) {
			return target;
		
		} else if (target.getClass().isPrimitive()) {
			return castInboundPrimitive(target, castType);
		
		} else {
			return castInboundObject(target, castType);
		}
	}
	
	
	private static Object castInboundPrimitive(Object target, Class castType) {
		Object objectifiedTarget = null;
		
		Class targetClass = target.getClass();
		
		if (targetClass == int.class) {
			objectifiedTarget = (Integer)target;
		} else if (targetClass == boolean.class) {
			objectifiedTarget = (Boolean)target;
		} else if (targetClass == char.class) {
			objectifiedTarget = (Character)target;
		} else if (targetClass == byte.class) {
			objectifiedTarget = (Byte)target;
		} else if (targetClass == short.class) {
			objectifiedTarget = (Short)target;
		} else if (targetClass == long.class) {
			objectifiedTarget = (Long)target;
		} else if (targetClass == float.class) {
			objectifiedTarget = (Float)target;
		} else if (targetClass == double.class) {
			objectifiedTarget = (Double)target;
		}
		
		return castInboundObject(objectifiedTarget, castType);
	}
	
	
	private static Object castInboundObject(Object target, Class castType) {
		String modifiedTarget = null;
		
		if (target.getClass() != Timestamp.class) {
			modifiedTarget = target.toString();
		} else {
			modifiedTarget = ((Timestamp)target).toString();
		}
		
		if (castType == String.class) { 
			return modifiedTarget;
		} else if (castType == Integer.class || castType == int.class) { 
			return Integer.parseInt(modifiedTarget);
		} else if (castType == Timestamp.class) { 
			return Timestamp.valueOf(modifiedTarget);
		} else if (castType == Double.class || castType == double.class) { 
			return Double.parseDouble(modifiedTarget);
		} else if (castType == Float.class || castType == float.class) { 
			return Float.parseFloat(modifiedTarget);
		} else if (castType == Long.class || castType == long.class) { 
			return Long.parseLong(modifiedTarget);
		} else if (castType == Boolean.class || castType == boolean.class) { 
			return Boolean.valueOf(modifiedTarget);
		} else if (castType == java.sql.Date.class) { 
			return java.sql.Date.valueOf(modifiedTarget);
		} else if (castType == java.util.Date.class) { 
			return java.util.Date.parse(modifiedTarget);
		} else if (castType == Character.class || castType == char.class) { 
			return modifiedTarget.toCharArray()[0];
		} else { 
			return target;
		}
	}
	
	
	public static String sanitizeString(String string) {
		Set<Integer> invalidCharacters = getInvalidCharCodes();
		
		return sanitizeString(invalidCharacters, string);
	}
	
	
	public static Set<Integer> getInvalidCharCodes() {
		if (invalidCharCodes == null) {
			invalidCharCodes = buildInvalidCharCodesSet();
		}
		
		return invalidCharCodes;
	}
	
	
	private static Set<Integer> buildInvalidCharCodesSet() {
		//The following character codes broke the JSON.parse function
		Integer[] invalidCharCodes = new Integer[]{
    		0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
    		21,22,23,24,25,26,27,28,29,30,31,192,193,194,195,196,
    		197,198,199,200,201,202,203,204,205,206,207,208,209,
    		210,211,212,213,214,215,216,217,218,219,220,221,222,
    		223,224,225,226,227,228,229,230,231,232,233,234,235,
    		236,237,238,239,240,241,242,243,244,245,246,247,248,
    		249,250,251,252,253
    	};
    	
    	Set<Integer> invalidCharCodesSet = new HashSet<Integer>();
    	
    	for (Integer charCode : invalidCharCodes) {
    		invalidCharCodesSet.add(charCode);
    	}
    	
    	return invalidCharCodesSet;
	}
	
	
	public static String sanitizeString(Set<Integer> invalidCharacters, String string) {
		StringBuffer sanitizedText = new StringBuffer();
		
		if (string != null) {
			for (int i=0; i<string.length(); i++) {
				int charCode = (int)string.charAt(i);
				
				if (!invalidCharacters.contains(charCode)) {
					sanitizedText.append((char)charCode);
				}
			}
		}
		
		return sanitizedText.toString();
	}
	
	
	public static void printJSON(String json, HttpServletResponse response) {
		PrintWriter pw = null;
    	
    	try {
	    	pw = response.getWriter();
	    	pw.println(json);
    	} catch(Exception e) {
    		e.printStackTrace();
    	} finally {
    		if (pw != null) {
    			pw.close();
    		}
    	}
	}
	
	
	public static void closeAutoCloseables(AutoCloseable... closeables) {
		for (AutoCloseable closeable : closeables) {
			try {
				if (closeable != null) {
					closeable.close();
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public static StringBuffer readFile(InputStream in, Encoder encoder) throws Exception {
		StringBuffer content = new StringBuffer();
		
		try {
			int byteArraySize = 0;
			
			while ((byteArraySize = in.available()) > 0) {			
				byte[] bytes = new byte[byteArraySize];
				
				in.read(bytes);
				
				content.append(encoder.encode(bytes));
			}
		} catch(Exception e) {
			throw e;
		} finally {
			in.close();
		}
		
		return content;
	}
	
	
	public static StringBuffer readBlob(ResultSet rs, String parameter) throws Exception {
		InputStream in = null;
		StringBuffer content = new StringBuffer();
		
		try {
			Blob blob = rs.getBlob(parameter);
			in = blob.getBinaryStream();
			
			content.append(Utils.readFile(in, (byte[] bytes) -> {
				return new String(bytes, "UTF-8");
			}));
		} finally {
			closeAutoCloseables(in);
		}
		
		return content;
	}
	
	
	public static boolean hasInputs(String... inputs) {
		for (String input : inputs) {
			if (input == null || input.isEmpty()) 
				return false;
		}
		
		return true;
	}
	
	
}
