package com.iteracja.database;


class QueryData {

	private boolean records;
	private String sql;
	private int index;

	/**
	 * Ustawienie danych do przechowania
	 * @param index indeks zapytania
	 * @param sql dane do przetworzenia
	 * @param records czy zapytanie zwraca rekordy
	 */
	QueryData(int index, String sql , boolean records) {
		this.sql=sql;
		this.records=records;
		this.index=index;
	}
	
	/**
	 * Sprawdzenie czy metoda zwraca rekordy
	 * @return true jeżeli tak, false jeżeli nie
	 */
	boolean isRecords(){
		return records;
	}
	
	/**
	 * Pobranie danych do przetworzenia
	 * @return dane do przetworzenia
	 */
	String getSql(){
		return sql;
	}

	public Integer getIndex() {
		return index;
	}
}
