package com.billings.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SQLStatementUtils {
	
	private Connection conn;
	private PreparedStatement ps;
	private ResultSet rs;
	
	public static void executeDelete(String query, Object... parameters) {
		executeUpdate(query, parameters);
	}
	
	
	public static void executeInsert(String query, Object... parameters) {
		executeUpdate(query, parameters);
	}
	
	
	public static void executeUpdate(String query, Object... parameters) {
		Connection conn = getConnection();
		PreparedStatement ps = prepareQuery(conn, query);
		
		addParams(conn, ps, parameters);
		
		executeStatement(ps);
		
		closeStreams(conn, ps);
	}
	
	
	public ResultSet executeQuery(String query) {
		return executeQuery(query, new Object[0]);
	}
	
	
	public ResultSet executeQuery(String query, Object... parameters) {
		this.conn = getConnection();
		this.ps = prepareQuery(conn, query);
		
		addParams(conn, ps, parameters);
		
		this.rs = getResultSetFromExecuteQuery(conn, ps);
		
		return rs;
	}
	
	
	public static Object executeQueryForSingleCell(String query) {
		return executeQueryForSingleCell(query, String.class);
	}
	
	
	public static Object executeQueryForSingleCell(String query, Class resultType) {
		return executeQueryForSingleCell(query, resultType, new Object[0]);
	}
	
	
	public static Object executeQueryForSingleCell(String query, Object... parameters) {
		return executeQueryForSingleCell(query, String.class, parameters);
	}
	
	
	public static Object executeQueryForSingleCell(String query, Class resultType, Object... parameters) {
		List resultList = executeQuery(query, HashMap.class, parameters);
		
		Map<String, Object> resultMap = (Map<String, Object>)getSingleObjectResult(resultList);
		
		Object singleResult = getSingleObjectFromMap(resultMap);
		
		Object result = Utils.cast(singleResult, resultType);
		
		return result; 
	}
	

	public static Object executeQueryForSingleRow(String query, Class resultType) {
		return executeQueryForSingleRow(query, resultType, new Object[0]);
	}
	
	
	private static Object getSingleObjectResult(List resultList) {
		int resultCount = resultList.size();
		
		Object obj = null;
		
		if (resultCount == 1) {
			obj = resultList.get(0);
		} 
		
		return obj;
	}
	
	
	private static Object getSingleObjectFromMap(Map<String, Object> resultMap) {
		Set<String> keys = resultMap.keySet();
		
		Object result = null;
		
		for (String key : keys) {
			result = resultMap.get(key);
		}
		
		return result;
	}
	
	
	public static Object executeQueryForSingleRow(String query, Class resultType, Object... parameters) {
		List resultList = executeQuery(query, resultType, parameters);
		
		 return getSingleObjectResult(resultList);
	}
	
	
	public static List executeQuery(String query, Class resultType) {
		return executeQuery(query, resultType, new Object[0]);
	}
	
	
	public static List executeQuery(String query, Class resultType, Object... parameters) {
		Connection conn = getConnection();
		PreparedStatement ps = prepareQuery(conn, query);
		
		addParams(conn, ps, parameters);
		
		ResultSet rs = getResultSetFromExecuteQuery(conn, ps);
		
		List dataList = getDataList(rs, resultType);
		
		closeStreams(conn, ps, rs);
		
		return dataList;
	}
	
	
	public static void executeBatchUpdate(String query, Object[]... arrayOfParameters) {
		Connection conn = getConnection();
		
		PreparedStatement ps = null;
		
		try {
			ps = prepareQuery(conn, query);
			
			executeBatchUpdate(query, conn, ps, arrayOfParameters);
		} catch (Exception e) {
			handleCatch(e, "Could not execute batch update");
		} finally {
			closeStreams(conn, ps);
		}
	}
	
	
	public static void executBatchCallableUpdate(String query, Object[]... arrayOfParameters) {
		Connection conn = getConnection();
		
		CallableStatement cs = null;
		
		try {
			cs = prepareCallableStatement(conn, query);
			
			executeBatchUpdate(query, conn, cs, arrayOfParameters);
		} catch (Exception e) {
			handleCatch(e, "Could not execute batch Callable Statement");
		} finally {
			closeStreams(conn, cs);
		}
	}
	
	
	private static void executeBatchUpdate(String query, Connection conn, PreparedStatement ps, Object[][] arrayOfParameters) throws Exception {		
		conn.setAutoCommit(false);
		
		for (Object[] parameters : arrayOfParameters) {
			addParams(conn, ps, parameters);
			
			ps.addBatch();
		}
		
		ps.executeBatch();
		
		conn.commit();
	}
	
	
	private static void handleCatch(Exception e, String message) {
		handleCatch(e, message, null, null);
	}
	
	
	private static void handleCatch(Exception e, String message, Connection conn, PreparedStatement ps) {
		System.out.println(message);
		e.printStackTrace();
		closeStreams(conn, ps);
	}
	
	
	public static void executeCallableStatement(String query) {
		executeCallableStatement(query, null, null, null);
	}
	
	
	public static void executeCallableStatement(String query, Object[] parameters) {
		executeCallableStatement(query, null, null, parameters);
	}

	
	public static Object executeCallableStatement(String query, int outParameterIndex, int outParameterType, Object[] parameters) {
		int[] outParameterIndexes = new int[]{ outParameterIndex };
		int[] outParameterTypes = new int[]{ outParameterType };
		
		Object[] results = executeCallableStatement(query, outParameterIndexes, outParameterTypes, parameters);
		
		Object result = null;
		
		if (results != null && results.length > 0) {
			result = results[0];
		}
		
		return result;
	}
	
	
	public static Object[] executeCallableStatement(String query, int[] outParameterIndexes, int[] outParameterTypes, Object[] parameters) {
		Connection conn = getConnection();
		
		CallableStatement cs = prepareCallableStatement(conn, query);
		
		if (outParameterTypes != null && outParameterTypes.length > 0) {
			registerOutParameters(conn, cs, outParameterIndexes, outParameterTypes);
			
			addParams(conn, cs, outParameterIndexes, parameters);
		} else {
			addParams(conn, cs, parameters);
		}
		
		executeStatement(cs);
		
		Object[] outputs = getCallableStatementResults(conn, cs, outParameterIndexes);
		
		closeStreams(conn, cs);
		
		return outputs;
	}
	
	
	private static Connection getConnection() {
		Connection conn = null;
		try {
			conn = DatabaseUtils.getConnection();
		} catch (Exception e) {
			handleCatch(e, "Couldn't obtain a connection", conn, null);
		}
		
		return conn;
	}
	
	
	private static PreparedStatement prepareQuery(Connection conn, String query) {
		PreparedStatement ps = null;

		try {
			ps = conn.prepareStatement(query);
		} catch(Exception e) {
			handleCatch(e, "Exception thrown in prepareQuery", conn, null);
		}
		
		return ps;
	}
	
	
	private static CallableStatement prepareCallableStatement(Connection conn, String query) {
		CallableStatement cs = null;
		
		try {
			cs = conn.prepareCall(query);
		} catch(Exception e) {
			handleCatch(e, "Exception thrown in prepareCallableStatement", conn, null);
		}
		
		return cs;
	}
	
	
	private static void addParams(Connection conn, PreparedStatement ps, Object[] parameters) {
		try {
			int index = 1;
			
			for (Object parameter : parameters) {
				ps.setObject(index++, parameter);
			}
			
		} catch(Exception e) {
			handleCatch(e, "Exception thrown in addParams", conn, ps);
		}
	}
	
	
	private static void addParams(Connection conn, PreparedStatement ps, int[] outParameterIndexes, Object[] parameters) {
		try {
			int statementParameterizedCount = parameters.length + outParameterIndexes.length;
			
			int parametersIndex = 0;
			
			Set<Integer> outParameters = new HashSet<Integer>();
			
			for (int index : outParameterIndexes) {
				outParameters.add(index);
			}
			
			for (int paramIndex=1; paramIndex <= statementParameterizedCount; paramIndex++) {
				if (!outParameters.contains(paramIndex)) {
					ps.setObject(paramIndex, parameters[parametersIndex++]);
				}
			}
			
		} catch(Exception e) {
			handleCatch(e, "Exception thrown in addParams", conn, ps);
		}
	}
	
	
	private static void registerOutParameters(Connection conn, CallableStatement cs, int[] indexes, int[] outParameterTypes) {		
		try {
			for (int i=0; i<indexes.length; i++) {
				cs.registerOutParameter(indexes[i], outParameterTypes[i]);
			}
		
		} catch(Exception e) {
			handleCatch(e, "Couldn't register out parameter", conn, cs);
		}
	}
	
	
	private static ResultSet getResultSetFromExecuteQuery(Connection conn, PreparedStatement ps) {
		ResultSet rs = null;
		
		try {
			rs = ps.executeQuery();
		} catch(Exception e) {
			handleCatch(e, "Couldn't obtain ResultSet from query", conn, ps);
		}
		
		return rs;
	}
	
	
	private static Object[] getCallableStatementResults(Connection conn, CallableStatement cs, int[] outParameterIndexes) {
		int indexesCount = outParameterIndexes.length;
		
		Object[] results = new Object[indexesCount];
		
		try {
			for (int i=0; i<indexesCount; i++) {
				results[i] = cs.getObject(outParameterIndexes[i]);
			}
			
		} catch(Exception e) {
			handleCatch(e, "Couldn't obtain results from callable statement", conn, cs);
		}
		
		return results;
	}
	

	@SuppressWarnings("rawtypes")
	private static List getDataList(ResultSet rs, Class resultType) {
		List dataList = null;
		
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			
			String[] columnNames = getColumnNames(rsmd);
			
			if (Map.class.isAssignableFrom(resultType)) {
				dataList = getMapDataList(rs, columnNames, resultType);
			} else {
				dataList = getObjectDataList(rs, columnNames, resultType);
			}
			
		} catch(Exception e) {
			handleCatch(e, "Could not populate dataList from resultSet");
		}
		
		return dataList;
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static List getMapDataList(ResultSet rs, String[] columnNames, Class resultType) throws Exception {
		List dataList = new ArrayList();
		
		Constructor constructor = resultType.getConstructor();
		
		while (rs.next()) {
			Map map = (Map)constructor.newInstance();
			
			for (String column : columnNames) {
				Object value = rs.getObject(column);

				map.put(column, value);
			}
			
			dataList.add(map);
		}

		return dataList;
	}
	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static List getObjectDataList(ResultSet rs, String[] columnNames, Class resultType) throws Exception {
		List dataList = new ArrayList();
		
		Method[] methodNames = Utils.setMethodsForAttributes(resultType, columnNames);
		
		Constructor constructor = resultType.getConstructor();
		
		int columnCount = columnNames.length;
		
		while(rs.next()) {
			Object result = constructor.newInstance();
			
			for (int i=0; i<columnCount; i++) {
				Object value = rs.getObject(columnNames[i]);
				
				Method currentMethod = methodNames[i];
				
				//Grab the first element only because the setter method only accepts one value
				Class inputType = currentMethod.getParameterTypes()[0]; 
				
				value = Utils.cast(value, inputType);

				methodNames[i].invoke(result, value);
			}
			
			dataList.add(result);
		}
		
		return dataList;
	}
	

	private static String[] getColumnNames(ResultSetMetaData rsmd) throws Exception {
		int columnCount = rsmd.getColumnCount();
		
		String[] columnNames = new String[columnCount];
		
		for (int i=0; i<columnCount; i++) {
			columnNames[i] = rsmd.getColumnLabel(i+1);
		}
		
		return columnNames;
	}
	
	
	private static void executeStatement(PreparedStatement ps) {
		try {
			ps.execute();
		} catch(Exception e) {
			handleCatch(e, "Could not execute update");
		}
	}
	
	
	public void closeStreams() {
		closeStreams(conn, ps, rs);
	}
	
	
	private static void closeStreams(Connection conn, PreparedStatement ps) {
		closeStreams(conn, ps, null);
	}
	
	
	private static void closeStreams(Connection conn, PreparedStatement ps, ResultSet rs) {
		closeConnection(conn);
		closeStatement(ps);
		closeResultSet(rs);
	}
	
	
	private static void closeStatement(PreparedStatement ps) {
		try {
			if (ps != null)
				ps.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private static void closeConnection(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void closeResultSet(ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void finalize() {
		closeStreams(conn, ps, rs);
	}
	
}
