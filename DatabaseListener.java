package com.iteracja.database;


public interface DatabaseListener {

	void error(Exception e);

	void connected(DatabaseConnector postgresql);

	void interuptedConnection(Postgresql postgresql);

}
