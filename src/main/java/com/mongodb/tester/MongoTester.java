package com.mongodb.tester;

import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "mongoTester", mixinStandardHelpOptions = true, version = "0.1", description = "MongoDB tester utility")
public class MongoTester implements Callable<Integer>, CommandListener {
	
	private static Logger logger = LoggerFactory.getLogger(MongoTester.class);
	
	@Option(names = { "--uri" }, description = "source mongodb uri connection string", required = true)
	private String uri;
	
	@Option(names = { "--db" }, description = "database name to test", required = false)
	private String databaseName;
	
	@Option(names = { "--collection" }, description = "collection name to test", required = false)
	private String collName;
	
	
	@Override
	public Integer call() throws Exception {
		
		logger.debug("MongoTester starting");
		
		ConnectionString connectionString = new ConnectionString(uri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
				.applyConnectionString(connectionString)
				.addCommandListener(this)
				.build();
		logger.debug("settings: {}", mongoClientSettings);
		MongoClient mongoClient = MongoClients.create(mongoClientSettings);
		
		if (databaseName == null) {
			
			MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
	        for (String dbName : dbNames) {
	        	
	        	if (dbName.equals("admin") || dbName.equals("config") || dbName.equals("local")) {
	        		continue;
	        	}
	        	
	        	MongoDatabase db = mongoClient.getDatabase(dbName);
	        	MongoIterable<String> collectionNames = db.listCollectionNames();
                for (String collectionName : collectionNames) {
                	MongoCollection<Document> coll = db.getCollection(collectionName);
                	Document first = coll.find().first();
                	
                	if (first == null) {
                		continue;
                	}
                	Object id = first.get("_id");                	
                	logger.debug("got doc from collection: {}, _id: {}", collectionName, id);
                }
	        	
	        }
			
		}
		
		return 0;
	}

	public static void main(String[] args) {
		int exitCode = new CommandLine(new MongoTester()).execute(args);
		System.exit(exitCode);
	}

	@Override
	public void commandStarted(CommandStartedEvent event) {
		String cmdName = event.getCommandName();
		ServerAddress serverAddress = event.getConnectionDescription().getServerAddress();
		String serverType = event.getConnectionDescription().getServerType().toString();
		logger.debug("command started, cmd: {}, server: {}, type: {}", cmdName, serverAddress, serverType);
	}

	@Override
	public void commandSucceeded(CommandSucceededEvent event) {
		String cmdName = event.getCommandName();
		ServerAddress serverAddress = event.getConnectionDescription().getServerAddress();
		logger.debug("command success, cmd: {}, server: {}", cmdName, serverAddress);
	}

	@Override
	public void commandFailed(CommandFailedEvent event) {
		String cmdName = event.getCommandName();
		ServerAddress serverAddress = event.getConnectionDescription().getServerAddress();
		logger.debug("command failure, cmd: {}, server: {}", cmdName, serverAddress);
	}

}
