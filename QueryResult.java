package com.iteracja.database;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryResult {

	private boolean success=true;
	private String errorMessage;
	private ResultSet result;
	private int cursor=-1;
	private String[] columns;
	private ArrayList<Object[]> records=new ArrayList<Object[]>();

	public boolean isSuccess() {
		return success;
	}

	public String getErrorMessage() {
		return errorMessage;
	}
	
	/**
	 * Ustawienie rezultatu zapytania
	 * @param result obiekt z danymi
	 */
	public void setResult(ResultSet result) {
		this.result=result;
		
	}

	
	public void setErrorMessage(String message) {
		this.errorMessage=message;
		success=false;
	}

	public ResultSet getResult() {
		return result;
	}

	public void newRecord() {
		cursor++;
		
	}

	public void addRecord(Object[] value) {
		
		records.add(value);
	
	}

	public void setColumns(String[] columnsData) {
		columns=columnsData;
	}

	public boolean next() {
		cursor++;
		if(records.size()>cursor)
			return true;
		else
			return false;
	}

	public String getString(String column) throws SQLException {
		int index=getColumnIndex(column);
		return String.valueOf(records.get(cursor)[index]);
	}

	private int getColumnIndex(String column) throws SQLException{
		
		for(int i=0; i < columns.length; i++){
			if(columns[i].equals(column))
				return i;
		}
		
		throw new SQLException("Column not found");
	}

	public void resetCursor() {
		cursor=-1;
	}

	public Boolean getBoolean(String column) throws SQLException {
		int index=getColumnIndex(column);
		if(records.get(cursor)[index]!=null)
			return Boolean.parseBoolean(records.get(cursor)[index].toString());
		else
			return null;
	}

	public int getInt(String column) throws SQLException {
		int index=getColumnIndex(column);
		return Integer.parseInt(records.get(cursor)[index].toString());
	}

	public Long getLong(String column) throws SQLException {
		int index=getColumnIndex(column);
		if(records.get(cursor)[index]!=null)
			return Long.parseLong(records.get(cursor)[index].toString());
		else 
			return null;
	}

	/**
	 * @author Michał Tomczak
	 * @return
	 */
	public HashMap<String, String> toHashMap() {
		HashMap<String, String> result=new HashMap<String, String>();
		Object[] row=records.get(cursor);
		for(int i=0; i <row.length; i++){
			result.put(columns[i], (String)row[i]);
		}
		return result;
	}


}
