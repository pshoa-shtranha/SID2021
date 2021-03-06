package SID2021.projetoSID;

import com.mongodb.client.*;


public class App2 {
	static String connectionString = "mongodb://aluno:aluno@194.210.86.10:27017/?authSource=admin";

	public static void main(String[] args) {
		try (MongoClient mongoClient = MongoClients.create(connectionString)) {

			MongoDatabase db = mongoClient.getDatabase("sid2021");
			MongoCollection mongoCollection = db.getCollection("sensorh1");
			
			  printDBNames(mongoClient); 
			  printCollections(db);
			  printNumDocs(mongoCollection);
			 
			
//			  Document objeto = new ; FindIterable<Document> objetos =
//			  mongoCollection.find();
//			  objetos.forEach(new Block<Document>() { public void apply(final Document
//			  document) { System.out.println(document.toJson()); } }
			 
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static void printNumDocs(MongoCollection mongoCollection) {
		long numdocs = mongoCollection.countDocuments();
		System.out.println(numdocs);
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