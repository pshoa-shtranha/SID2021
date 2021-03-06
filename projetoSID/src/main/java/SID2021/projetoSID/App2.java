package SID2021.projetoSID;

import com.mongodb.client.*;
import org.bson.Document;

public class App2 {
	static String connectionString = "mongodb://aluno:aluno@194.210.86.10:27017/?authSource=admin";

    private static final int MAX_REGISTERS = 5000;
	
	public static void main(String[] args) {
		try (MongoClient mongoClient = MongoClients.create(connectionString)) {

			MongoDatabase db = mongoClient.getDatabase("sid2021");
			MongoCollection<Document> mongoCollection = db.getCollection("sensorh1");
			
			printDocsInCollection(mongoCollection);
//			  printDBNames(mongoClient); 
//			  printCollections(db);
//			  printNumDocs(mongoCollection);
			 
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static void printDocsInCollection(MongoCollection<Document> mongoCollection) {
		MongoCursor<Document> docCursor = mongoCollection.find().iterator();
		
		int count = 0;
		while (docCursor.hasNext() && count < MAX_REGISTERS) {
		    Document doc = docCursor.next();
		    System.out.println(doc.toJson());
		    
		    count++;
		}
	}

	private static void printNumDocs(MongoCollection mongoCollection) {
		System.out.println(mongoCollection.countDocuments());
	}

	private static void printCollections(MongoDatabase db) {
		for (String name : db.listCollectionNames()) {
			System.out.println(name);
		}
	}

	private static void printDBNames(MongoClient mongoClient) {
		mongoClient.listDatabaseNames();
		MongoIterable<String> strings = mongoClient.listDatabaseNames();
		MongoCursor<String> cursor = strings.cursor();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
}