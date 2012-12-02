package com.iteracja.database;


public interface CollectorDatabaseListener {

	void connected(Object postgresql);

	void close(Object postgresql);

	void interruptedConnection(Object postgresql);

}
