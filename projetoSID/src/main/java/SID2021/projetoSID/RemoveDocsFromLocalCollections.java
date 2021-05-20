package SID2021.projetoSID;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class RemoveDocsFromLocalCollections {
	static String connectionStringLocal = "mongodb://localhost:27027,localhost:25017,localhost:23017/?replicaSet=culturas";

	public static void main(String[] args) {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
		try (MongoClient localClient = MongoClients.create(connectionStringLocal)) {
			MongoDatabase localdb = localClient.getDatabase("sid2021");
			BasicDBObject query = new BasicDBObject();
			MongoCollection<Document> localCollection = localdb.getCollection("sensorh1");
			MongoCollection<Document> localCollection2 = localdb.getCollection("sensorh2");
			MongoCollection<Document> localCollection3 = localdb.getCollection("sensorl1");
			MongoCollection<Document> localCollection4 = localdb.getCollection("sensorl2");
			MongoCollection<Document> localCollection5 = localdb.getCollection("sensort1");
			MongoCollection<Document> localCollection6 = localdb.getCollection("sensort2");
			localCollection.deleteMany(query);
			localCollection2.deleteMany(query);
			localCollection3.deleteMany(query);
			localCollection4.deleteMany(query);
			localCollection5.deleteMany(query);
			localCollection6.deleteMany(query);
			System.out.println("Apagado com sucesso");
		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
