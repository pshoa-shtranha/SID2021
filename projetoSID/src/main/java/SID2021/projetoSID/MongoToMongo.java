package SID2021.projetoSID;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
//hey
public class MongoToMongo {
	static String remote_mongo_address;
	static String remote_mongo_authentication;
	static String remote_mongo_user;
	static String remote_mongo_password;
	static String remote_db;
	static String local_replica_set;
	static String local_replica_name;
	static String local_mongo_address;
	static String local_mongo_authentication;
	static String local_mongo_user;
	static String local_mongo_password;
	static String local_db;
	static String continue_transfer_where_left;
	static String clear_local_collections_before_start;
	static String sleep_frequency_in_milliseconds;
	static String load_progress;
	static JTextArea documentLabel;

	static String connectionStringRemote;
	static String connectionStringLocal;
	static String[] remoteCollections = new String[1000];
	static String[] localCollections = new String[1000];
	static int numRemoteCollections;



	public static void main(String[] args) {
		// Desativar mensagens vermelhas irritantes
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
		createWindow();
		
		// carregar propriedades do .ini
		try {
			final Properties properties = new Properties();
			properties.load(MongoToMongo.class.getResourceAsStream("/MongoToMongo.ini"));

			MongoToMongo.remote_mongo_address = properties.getProperty("remote_mongo_address");
			MongoToMongo.remote_mongo_authentication = properties.getProperty("remote_mongo_authentication");
			MongoToMongo.remote_mongo_user = properties.getProperty("remote_mongo_user");
			MongoToMongo.remote_mongo_password = properties.getProperty("remote_mongo_password");
			MongoToMongo.remote_db = properties.getProperty("remote_db");
			MongoToMongo.local_replica_set = properties.getProperty("local_replica_set");
			MongoToMongo.local_replica_name = properties.getProperty("local_replica_name");
			MongoToMongo.local_mongo_address = properties.getProperty("local_mongo_address");
			MongoToMongo.local_mongo_authentication = properties.getProperty("local_mongo_authentication");
			MongoToMongo.local_mongo_user = properties.getProperty("local_mongo_user");
			MongoToMongo.local_mongo_password = properties.getProperty("local_mongo_password");
			MongoToMongo.local_db = properties.getProperty("local_db");
			MongoToMongo.clear_local_collections_before_start = properties
					.getProperty("clear_local_collections_before_start");
			MongoToMongo.sleep_frequency_in_milliseconds = properties.getProperty("sleep_frequency_in_milliseconds");
			MongoToMongo.load_progress = properties.getProperty("load_progress");
			readRemoteCollections(properties);
			readLocalCollections(properties);
		} catch (Exception e1) {
			System.out.println("Error reading MongoToMongo.ini file " + e1);
			JOptionPane.showMessageDialog(null, "The MongoToMongo inifile wasn't found.", "Mongo To Mongo", 0);
		}
			
			//criar e iniciar Threads
			Thread[] threads = new Thread[numRemoteCollections + 1];
			MongoToMongo mongoToMongo = new MongoToMongo();
			for (int i = 0; i < numRemoteCollections; i++) {
				threads[i] = mongoToMongo.new incrementador(i);
				try {
				threads[i].start();
				} catch (Exception e) {
					MongoToMongo.documentLabel.append("Nao foi possivel iniciar a Thread " + i + "\n");
					System.out.println(e);
				}
			}

	}

	private static void createWindow() {
		documentLabel = new JTextArea("\n");
		final JFrame frame = new JFrame("Mongo to Mongo");
		frame.setDefaultCloseOperation(3);
		final JLabel comp = new JLabel("Data from remote mongo: ", 0);
		comp.setPreferredSize(new Dimension(600, 30));
		final JScrollPane comp2 = new JScrollPane(MongoToMongo.documentLabel, 22, 32);
		comp2.setPreferredSize(new Dimension(600, 200));
		final JButton comp3 = new JButton("Stop the program");
		frame.getContentPane().add(comp, "First");
		frame.getContentPane().add(comp2, "Center");
		frame.getContentPane().add(comp3, "Last");
		frame.setLocationRelativeTo(null);
		frame.pack();
		frame.setVisible(true);
		comp3.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent actionEvent) {
				System.exit(0);
			}
		});
	}
	
	//lê quais são as coleções remotas a transferir do .ini
	private static void readRemoteCollections(final Properties properties) {
		String collectionName = "something";
		numRemoteCollections = 0;
		collectionName = properties.getProperty("remote_collection" + Integer.toString(numRemoteCollections + 1));
		while (collectionName != null) {
			remoteCollections[numRemoteCollections] = collectionName;
			numRemoteCollections++;
			collectionName = properties.getProperty("remote_collection" + Integer.toString(numRemoteCollections + 1));
		}
	}
	
	//lê quais são as coleções locais do .ini
	private static void readLocalCollections(final Properties properties) {
		String collectionName = "something";
		for (int i = 0; i < numRemoteCollections; i++) {
			if (collectionName == null || collectionName.isEmpty()) {
				System.out.println("number of local collections must be equal to number of remote collections");
				System.exit(0);
			}
			collectionName = properties.getProperty("local_collection" + Integer.toString(i + 1));
			localCollections[i] = collectionName;
		}
	}

	//inicia thread para cada conjunto coleção remota-local
	public class incrementador extends Thread {

		public int colecaoID;

		incrementador(int colecaoID) {

			this.colecaoID = colecaoID;
		}

		public void run() {

			new MongoToMongo();
			MongoToMongo.connectToMongos(colecaoID);
		}
	}

	//coneta-se aos mongos remotos e locais e efetua a transferência dos ficheiros
	private static void connectToMongos(int colecaoID) {
		makeRemoteConnectionString();
		makeLocalConnectionString();

		try {
			MongoClient remoteClient;
			synchronized (MongoToMongo.class) {
				remoteClient = MongoClients.create(connectionStringRemote);
			}
			try {
				MongoClient localClient;
				synchronized (MongoToMongo.class) {
					System.out.println("Thread " + Integer.toString(colecaoID + 1) + " connected to remote " + remoteCollections[colecaoID] + " and to local " + localCollections[colecaoID] + "\n");
					localClient = MongoClients.create(connectionStringLocal);
				}
				MongoToMongo.documentLabel.append("Thread " + Integer.toString(colecaoID + 1) + " connected to remote " + remoteCollections[colecaoID] + " and to local " + localCollections[colecaoID] + "\n");
				MongoDatabase remotedb = remoteClient.getDatabase(remote_db);
				MongoDatabase localdb = localClient.getDatabase(local_db);

				// apaga documentos de coleções locais antes de iniciar a transferência consoante o .ini
				if (clear_local_collections_before_start.equals("true")) {
					deleteDocsFromLocalCollection(localdb, colecaoID);
				}
				
				transferDocs(remotedb, localdb, remoteCollections, localCollections, colecaoID);
			} catch (Exception e) {
				MongoToMongo.documentLabel.append("Nao foi possivel conectar ao servidor mongo local!\n");
			}
		} catch (Exception e) {
			MongoToMongo.documentLabel.append("Nao foi possivel conectar ao servidor mongo remoto!\n");
		}

	}

	//cria a string usada para conetar-se ao servidor mongo remoto
	private static void makeRemoteConnectionString() {
		connectionStringRemote = "mongodb://";
		if (remote_mongo_authentication.equals("true")) {
			connectionStringRemote += remote_mongo_user + ":" + remote_mongo_password + "@";
		}

		connectionStringRemote += remote_mongo_address;

		if (remote_mongo_authentication.equals("true")) {
			connectionStringRemote += "/?authSource=admin";
		}
	}

	//cria a string usada para conetar-se ao servidor mongo local
	private static void makeLocalConnectionString() {
		connectionStringLocal = "mongodb://";
		if (local_mongo_authentication.equals("true")) {
			connectionStringLocal += local_mongo_user + ":" + local_mongo_password + "@";
		}

		connectionStringLocal += local_mongo_address;

		if (local_replica_set.equals("true")) {
			if (local_mongo_authentication.equals("true")) {
				connectionStringLocal += "/?replicaSet=" + local_replica_name + "&authSource=admin";
			} else {
				connectionStringLocal += "/?replicaSet=" + local_replica_name;
			}

		}

		else if (local_mongo_authentication.equals("true")) {
			connectionStringLocal += "/?authSource=admin";
		}
	}

	//transfere os documentos do mongo remoto para o mongo local
	private static void transferDocs(MongoDatabase remotedb, MongoDatabase localdb, String[] remoteCollections,
			String[] localCollections, int colecaoID) {
		MongoCollection<Document> remoteCollection = remotedb.getCollection(remoteCollections[colecaoID]);
		MongoCollection<Document> localCollection = localdb.getCollection(localCollections[colecaoID]);
//		System.out.println(remoteCollection.countDocuments());
		int i = 0;
		int count = 0;
		MongoCursor<Document> remoteDocCursor = remoteCollection.find().iterator();

		//carrega o número de documentos a saltar no iteravel (em princípio já transferidos)
		if (load_progress.equals("true") && !clear_local_collections_before_start.equals("true")) {
			count = loadProgressFromFile(remoteCollections, colecaoID);
			remoteDocCursor = remoteCollection.find().skip(count).iterator();
		}

		while (i == 0) {
			while (remoteDocCursor.hasNext()) {
				Document remoteDoc = remoteDocCursor.next();
				// Verificar se já tem doc na db local
				BasicDBObject queryDuplicates = new BasicDBObject();
				queryDuplicates.put("_id", remoteDoc.getObjectId("_id"));
				long duplicates = localCollection.count(queryDuplicates);
				if (duplicates == 0) {
					localCollection.insertOne(remoteDoc);
//					System.out.println("Inserted object with id " + remoteDoc.getObjectId("_id").toString() + " into "
//							+ localCollections[colecaoID]);
					if (count % 100 == 0) {
						// guarda o número de documentos já transferidos a cada 100 documentos transferidos
						saveProgress(remoteCollections, colecaoID, remoteDoc, localCollection.countDocuments());
						MongoToMongo.documentLabel.append(localCollections[colecaoID] + " has " + localCollection.countDocuments() + " objects\n");
						System.out.println(localCollections[colecaoID] + " has " + localCollection.countDocuments() + " objects\n");
					}
					count++;
//					System.out.println("coleção: " + localCollections[colecaoID]  + "\nID:_" + remoteDoc.getObjectId("_id").toString() + "\ncount: " + count + "\n");
				}
			}
			//já percorreu todos os documentos existentes, refaz iterável e dorme x segundos do .ini
			try {
				System.out.println("All objects in " + remoteCollections[colecaoID] + " transfered with success");
				MongoToMongo.documentLabel.append("All objects in " + remoteCollections[colecaoID] + " transfered with success");
				// reload cursor to account for new docs
				remoteDocCursor.close();
				count = loadProgressFromFile(remoteCollections, colecaoID);
				remoteDocCursor = remoteCollection.find().skip(count).iterator();
				Thread.sleep(Integer.parseInt(sleep_frequency_in_milliseconds));
			} catch (InterruptedException e) {
				MongoToMongo.documentLabel.append(e.toString());
				e.printStackTrace();
			}
		}
	}

	//carrega numero de docs a saltar no iteravel de um ficheiro
	private static int loadProgressFromFile(String[] remoteCollections, int colecaoID) {
		int count = 0;
		try {
			File file = new File(remote_db + "_" + remoteCollections[colecaoID] + "_" + local_db + localCollections[colecaoID] + "_savefile.txt");
			if (file.exists()) {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String currentLine;

				while ((currentLine = reader.readLine()) != null) {
					if (currentLine.contains(remote_db + ";" + remoteCollections[colecaoID])) {
						String[] split = currentLine.split(";");
						count = Integer.parseInt(split[2].trim());
						System.out.println("Progress loaded for " + remoteCollections[colecaoID] + "\n");
						MongoToMongo.documentLabel.append("Progress loaded for " + remoteCollections[colecaoID] + "\n");
					}
				}
				reader.close();

			}
		} catch (IOException e) {
			MongoToMongo.documentLabel
					.append("Não foi possível carregar o progresso para " + remoteCollections[colecaoID] + "\n");
			e.printStackTrace();
		}
		return count;
	}

	//guarda numero de docs já transferidos para conjunto coleção remota-local
	private static void saveProgress(String[] remoteCollections, int colecaoID, Document remoteDoc, long count) {
		try {
			File file = new File(remote_db + "_" + remoteCollections[colecaoID] + "_" + local_db + localCollections[colecaoID] + "_savefile.txt");
			// if file doesn't exist create it
			if (!file.exists()) {
				file.createNewFile();
			}

			BufferedReader reader = new BufferedReader(new FileReader(file));
			BufferedWriter removeLine = new BufferedWriter(new FileWriter(file));
			BufferedWriter appendLine = new BufferedWriter(new FileWriter(file, true));
			String currentLine;

			Boolean exists = false;
			while ((currentLine = reader.readLine()) != null) {
				if (currentLine.contains(remote_db + ";" + remoteCollections[colecaoID])) {
					// faz com que se apagar a linha guardada tem que obrigatoriamente criar uma
					// linha nova antes de se poder parar o programa
					synchronized (MongoToMongo.class) {
						removeLine.write(currentLine + System.getProperty("line.separator"));
						appendLine.append(remote_db + ";" + remoteCollections[colecaoID] + ";"
								+ Long.toString(count - 1) + "\n");
					}
					exists = true;
				}
			}
			if (exists.equals(false)) {
				appendLine.append(
						remote_db + ";" + remoteCollections[colecaoID] + ";" + Long.toString(count - 1) + "\n");
			}
			removeLine.close();
			appendLine.close();
			reader.close();
		} catch (IOException e) {
			MongoToMongo.documentLabel
					.append("Nao foi possivel guardar o progresso para " + remoteCollections[colecaoID] + "\n");
			e.printStackTrace();
		}
	}

	//apaga todos os documentos da coleção local a que a thread está conetada
	private static void deleteDocsFromLocalCollection(MongoDatabase localdb, int colecaoID) {
		BasicDBObject query = new BasicDBObject();
		MongoCollection<Document> localCollection = localdb.getCollection(localCollections[colecaoID]);
		localCollection.deleteMany(query);
		System.out.println("Coleção local " + localCollections[colecaoID] + " limpa");
		MongoToMongo.documentLabel.append("Coleção local " + localCollections[colecaoID] + " limpa\n");
	}
}
