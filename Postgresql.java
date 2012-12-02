package com.iteracja.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

public class Postgresql implements DatabaseConnector {

	private String host;
	private int port;
	private String username;
	private String password;
	private String database;
	private Connection connect;
	private int timeout;
	private LinkedList<QueryData> query = new LinkedList<QueryData>();
	private HashMap<Integer, QueryResult> results = new HashMap<Integer, QueryResult>();
	private int maxQuery = 0;

	private ArrayList<DatabaseListener> listener = new ArrayList<DatabaseListener>();
	private int reconnectCountQuery;
	protected int countQuery;
	protected boolean forceQuery;

	/**
	 * Ustawienie parametrów do połączenia
	 * @param host adres bazy
	 * @param port port bazy
	 * @param username nazwa użytkownika
	 * @param password hasło
	 * @param database nazwa bazy
	 */
	public Postgresql(String host, int port, String username, String password, String database) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
	}

	/**
	 * Połączenie z bazą
	 * @param timeout maksymalny czas na wykonanie sql. jeżeli czas minie to rozłącza bazę
	 * @param reconnectCountQuery co ile zapytań ma rozłączyć i połączyć ponownie z bazą w celu zwolnienia bufora
	 * @throws SQLException problem z połączeniem
	 */
	public void connect(int timeout, int reconnectCountQuery) throws SQLException {
		this.timeout = timeout;
		this.reconnectCountQuery = reconnectCountQuery;
		try {

			if (connect != null && !connect.isClosed())
				throw new DatabaseConnectException("Dabase is connected.");

			String url = "jdbc:postgresql://" + host + ":" + port + (database == null ? "" : "/" + database);
			Properties prop = new Properties();
			if (username != null)
				prop.setProperty("user", username);
			if (password != null)
				prop.setProperty("password", password);
			if (timeout > 0)
				prop.setProperty("socketTimeout", String.valueOf(timeout));
			this.connect = DriverManager.getConnection(url, prop);
			countQuery = 0;

			stack();
			// wywołanie listenera dla akcji
			for (DatabaseListener currentListener : listener.toArray(new DatabaseListener[listener.size()])) {

				try {
					currentListener.connected(this);
				} catch (Exception e) {
					for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
						currentListenerError.error(e);
					}
				}
			}

		} catch (Exception e) {

			for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
				currentListenerError.error(e);
			}

			throw new SQLException(e.getMessage());
		}

	}

	/**
	 * Dodanie listenera do obsługi zadań pobocznych
	 * @param listener instancja listenera
	 */
	public void addDatabaseListener(DatabaseListener listener) {
		this.listener.add(listener);
	}

	/**
	 * Sprawdzenie czy jest połączenie z bazą
	 * @return prawda jeżeli tak, fałsz jeżeli nie
	 */
	public boolean isConnected() {
		try {
			if (connect != null && !connect.isClosed())
				return true;
			else
				return false;

		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Wykonanie zapytania bez zwracania rekordów
	 * @param sql zapytanie
	 * @throws SQLException problem z bazą lub błąd zapytania
	 */
	public void execute(String sql) throws SQLException {

		if (!forceQuery && !isConnected())
			throw new SQLException("Database is disconnected");
		int index = 0;
		synchronized (query) {
			index = maxQuery++;
			query.add(new QueryData(index, sql, false));
			query.notify();
		}

		QueryResult qr = null;

		synchronized (results) {

			while (!results.containsKey(index)) {

				try {
					results.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			qr = results.get(index);
			results.remove(index);
		}

		if (!qr.isSuccess())
			throw new SQLException(qr.getErrorMessage());
	}

	/**
	 * Wykonanie zapytania ze zwróconymi rekordami
	 * @param sql zapytanie
	 * @throws SQLException problem z bazą lub błąd zapytania
	 * @return kontener danych
	 */
	public QueryResult executeQuery(String sql) throws SQLException {

		if (!forceQuery && !isConnected())
			throw new SQLException("Database is disconnected");
		int index = 0;
		synchronized (query) {
			index = maxQuery++;
			query.add(new QueryData(index, sql, true));
			query.notify();
		}

		QueryResult qr = null;

		synchronized (results) {

			while (!results.containsKey(index)) {

				try {
					results.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			qr = results.get(index);
			results.remove(index);
		}

		if (!qr.isSuccess())
			throw new SQLException(qr.getErrorMessage());

		return qr;
	}

	/**
	 * Rozłączenie z bazą
	 * @throws SQLException nie można rozłaczyć z bazą, np nie jest połączona
	 */
	public void disconnect() throws SQLException {

		connect.close();
	}

	/**
	 * Ponowne połączenie z bazą w momencie kiedy rozłączy bez generowania eventu.
	 * @param reconnectTime co ile czasu ma ponownie próbować połączyć
	 * @param events flaga sprawdzająca czy ma generować eventy o połączeniu
	 * @throws InterruptedException zły zakres dla reconnectTime
	 */
	public void reconnect(int reconnectTime, boolean events) throws InterruptedException {

		try {

			if (connect != null && !connect.isClosed())
				throw new DatabaseConnectException("Dabase is connected.");

			String url = "jdbc:postgresql://" + host + ":" + port + (database == null ? "" : "/" + database);
			Properties prop = new Properties();
			if (username != null)
				prop.setProperty("user", username);
			if (password != null)
				prop.setProperty("password", password);
			if (timeout > 0)
				prop.setProperty("socketTimeout", String.valueOf(timeout));
			this.connect = DriverManager.getConnection(url, prop);
			countQuery = 0;

			// wywołanie listenera dla akcji
			if (events) {
				for (DatabaseListener currentListener : listener.toArray(new DatabaseListener[listener.size()])) {

					try {
						currentListener.connected(this);
					} catch (Exception e) {
						for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
							currentListenerError.error(e);
						}
					}
				}
			}

		} catch (Exception e) {

			for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
				currentListenerError.error(e);
			}

			Thread.sleep(reconnectTime);
			reconnect(reconnectTime);
		}

	}

	/**
	 * Ponowne połączenie z bazą w momencie kiedy rozłączy.
	 * @param reconnectTime co ile czasu ma ponownie próbować połączyć
	 * @throws InterruptedException zły zakres dla reconnectTime
	 */
	public void reconnect(int reconnectTime) throws InterruptedException {
		reconnect(reconnectTime, true);
	}

	private QueryData getQueryData() {
		QueryData qd = null;
		synchronized (query) {
			while (true) {
				qd = query.poll();
				if (qd == null) {
					try {
						query.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				} else
					break;
			}
		}

		return qd;
	}

	private void stack() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					QueryData qd = getQueryData();
					QueryResult qr = new QueryResult();
					try {

						// sprawdzenie czy zrobić reconnect aby zwolnić bufor
						if (countQuery++ > reconnectCountQuery) {
							forceQuery = true;
							try {
								disconnect(); // TODO poprawić wrzucać w tej metodzie info o rozłączeniu z bazą
							} catch (Exception e) {
								for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
									try {
										currentListenerError.error(e);
									} catch (Exception e1) {
									}
								}

							}

							try {
								reconnect(10000, false);
							} catch (Exception e) {
								for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
									try {
										currentListenerError.error(e);
									} catch (Exception e1) {
									}
								}
							}

							forceQuery = false;
						}

						Statement statement = connect.createStatement();
						if (qd.isRecords()) {

							ResultSet res = statement.executeQuery(qd.getSql());
							ResultSetMetaData columns = res.getMetaData();
							String columnsData[] = new String[columns.getColumnCount()];
							for (int i = 1; i <= columns.getColumnCount(); i++) {
								columnsData[i - 1] = columns.getColumnName(i);
							}

							qr.setColumns(columnsData);

							while (res.next()) {
								qr.newRecord();
								Object values[] = new Object[columns.getColumnCount()];
								for (int i = 1; i <= columns.getColumnCount(); i++) {
									values[i - 1] = res.getObject(i);
								}
								qr.addRecord(values);
							}

							qr.resetCursor();
						} else
							statement.execute(qd.getSql());

						statement.close();
					} catch (SQLException e) {

						qr.setErrorMessage(e.getMessage());
						// uruchomienie listenera z błędem sql
						for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
							try {
								currentListenerError.error(e);
							} catch (Exception e1) {
							}
						}

						// jeżeli jest to rozlączenie bazy to wygeneruj kolejny wyjątek o rozłączeniu
						if (e.getMessage().substring(0, 5).equals("FATAL")) {

							for (DatabaseListener currentListenerError : listener.toArray(new DatabaseListener[listener.size()])) {
								try {
									currentListenerError.interuptedConnection(Postgresql.this);
								} catch (Exception e1) {
								}
							}

						}
					}
					synchronized (results) {
						results.put(qd.getIndex(), qr);
						results.notifyAll();
					}
				}

			}
		}).start();
	}

}
