package com.iteracja.database;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryResult {

	private boolean success=true;
	private String errorMessage;
	private ResultSet result;
	private int cursor=-1;
	private Statement statement;
	private int errorCode;
	private SQLException exception;

	public boolean isSuccess() {
		return success;
	}
	
	/**
	 * Ustawienie rezultatu zapytania
	 * @param result obiekt z danymi
	 */
	public void setResult(ResultSet result) {
		this.result=result;		
	}

	
	public ResultSet getResult() {
		return result;
	}

	public void newRecord() {
		cursor++;
		
	}

	public boolean next() throws SQLException {
		return result.next();
	}

	public String getString(String column) throws SQLException {
		return result.getString(column);
	}

	public Boolean getBoolean(String column) throws SQLException {
		return result.getBoolean(column);
	}

	public int getInt(String column) throws SQLException {
		return result.getInt(column);
	}

	public Long getLong(String column) throws SQLException {
		return result.getLong(column);
	}

	/**
	 * @author Michał Tomczak
	 * @return
	 * @throws SQLException 
	 */
	public HashMap<String, String> toHashMap() throws SQLException {

		HashMap<String, String> resultHashMap=new HashMap<String, String>();

		ResultSetMetaData columns = result.getMetaData();
		for (int i = 1; i <= columns.getColumnCount(); i++) {
			resultHashMap.put(columns.getColumnName(i), result.getString(i));
		}

		return resultHashMap;
	}

	void setStatement(Statement statement) {
		this.statement=statement;
	}

	public void setException(SQLException e) {
		this.exception=e;		
		this.success=false;
	}

	public SQLException getException() {
		return exception;
	}


}
