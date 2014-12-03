package com.polyglot.mongodb;

import java.net.UnknownHostException;
import java.util.Arrays;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class Mongo {

	public void test() throws UnknownHostException {
		MongoClient mongoClient = getClient(); 
		DB db = mongoClient.getDB( "mydb" );
	}
	
	public MongoClient getClient() throws UnknownHostException {
		MongoClient mongoClient = new MongoClient();
		mongoClient = new MongoClient( "localhost" );
		mongoClient = new MongoClient( "localhost" , 27017 );
		
		// or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
		mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017),
		                                      new ServerAddress("localhost", 27018),
		                                      new ServerAddress("localhost", 27019)));
		
		return mongoClient;
	}
}
