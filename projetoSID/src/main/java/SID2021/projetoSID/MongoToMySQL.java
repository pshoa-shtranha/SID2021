package SID2021.projetoSID;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import twitter4j.JSONObject;

public class MongoToMySQL {
	static JTextArea documentLabel;
	static String remote_mongo_address;
	static String remote_mongo_authentication;
	static String remote_mongo_user;
	static String remote_mongo_password;
	static String remote_db;
	static String server_local;
	static String user;
	static String password;
	static String sleep_frequency_in_milliseconds;
	static String delete_many;
	
	static int numRemoteCollections;
	static String[] remoteCollections = new String[1000];
	static String connectionStringRemote;
	
	static Connection sqlConnection = null;
	static int numberSent = 0;
	
	static long timeCounter = 0;
	
	public static void main(String[] args) {
		// Desativar mensagens vermelhas irritantes
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
		createWindow();
	}
	
	private static void createWindow() {
		documentLabel = new JTextArea("\n");
		final JFrame frame = new JFrame("Mongo to MySQL");
		frame.setDefaultCloseOperation(3);
		final JLabel comp = new JLabel("Data from remote mongo: ", 0);
		comp.setPreferredSize(new Dimension(600, 30));
		final JScrollPane comp2 = new JScrollPane(MongoToMySQL.documentLabel, 22, 32);
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
	
	// carregar propriedades do .ini
		try {
			final Properties properties = new Properties();
			properties.load(MongoToMySQL.class.getResourceAsStream("/MongoToMySQL.ini"));

			MongoToMySQL.remote_mongo_address = properties.getProperty("remote_mongo_address");
			MongoToMySQL.remote_mongo_authentication = properties.getProperty("remote_mongo_authentication");
			MongoToMySQL.remote_mongo_user = properties.getProperty("remote_mongo_user");
			MongoToMySQL.remote_mongo_password = properties.getProperty("remote_mongo_password");
			MongoToMySQL.remote_db = properties.getProperty("remote_db");
			MongoToMySQL.server_local = properties.getProperty("server_local");
			MongoToMySQL.user = properties.getProperty("user");
			MongoToMySQL.password = properties.getProperty("password");
			MongoToMySQL.sleep_frequency_in_milliseconds = properties.getProperty("sleep_frequency_in_milliseconds");
			MongoToMySQL.delete_many = properties.getProperty("delete_many");
			readRemoteCollections(properties);
		} catch (Exception e1) {
			System.out.println("Error reading MongoToMySQL.ini file " + e1);
			JOptionPane.showMessageDialog(null, "The MongoToMySQL inifile wasn't found.", "Mongo To MySQL", 0);
		}
		
		connectToMySQL();
		// deletes MySQL entries if specified in ini file
		if(MongoToMySQL.delete_many.equals("true") && sqlConnection != null) {
			clearMySQLEntries();
		}
		
		//criar e iniciar Threads
		Thread[] threads = new Thread[numRemoteCollections + 1];
		MongoToMySQL mongoToMySQL = new MongoToMySQL();
		for (int i = 0; i < numRemoteCollections; i++) {
			threads[i] = mongoToMySQL.new incrementador(i, remoteCollections);
			try {
			threads[i].start();
			} catch (Exception e) {
				MongoToMySQL.documentLabel.append("Nao foi possivel iniciar a Thread " + i + "\n");
				System.out.println(e);
			}
		}
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
	
	//inicia thread para cada conjunto coleção remota-local
	public class incrementador extends Thread {

		public int colecaoID;
		public String[] remoteCollections;
		public int count;
		
		incrementador(int colecaoID, String[] remoteCollections) {

			this.colecaoID = colecaoID;
			this.remoteCollections = remoteCollections;
		}

		public void run() {

			new MongoToMySQL();
			connectToMongoAndMySQL();
		}
		
		//coneta-se ao mongo remoto e ao MySQL local e efetua a transferência
		private void connectToMongoAndMySQL() {
			makeRemoteConnectionString();
			try {
				MongoClient remoteClient;
				synchronized (MongoToMySQL.class) {
					remoteClient = MongoClients.create(connectionStringRemote);
				}			

				MongoDatabase remotedb = remoteClient.getDatabase(remote_db);
				System.out.println("Thread " + Integer.toString(colecaoID + 1) + " connected to remote " + remoteCollections[colecaoID]);
				MongoToMySQL.documentLabel.append("Thread " + Integer.toString(colecaoID + 1) + " connected to remote " + remoteCollections[colecaoID] + "\n");
				
				transferDocs(remotedb, sqlConnection);
				
			} catch (Exception e) {
				MongoToMySQL.documentLabel.append("Nao foi possivel conectar ao servidor mongo remoto!\n");
				e.printStackTrace();
			}

		}
		
		private void transferDocs(MongoDatabase remotedb, Connection sqlConnection) {
			MongoCollection<Document> remoteCollection = remotedb.getCollection(remoteCollections[colecaoID]);
//			System.out.println(remoteCollection.countDocuments());
			int i = 0;
			int count = 0;
			MongoCursor<Document> remoteDocCursor = remoteCollection.find().iterator();

			while (i == 0) {
				while (remoteDocCursor.hasNext()) {
					Document remoteDoc = remoteDocCursor.next();
					//converto to Json and then put in mysql
					String jsonString = "{id:" + count + ", doc:" + remoteDoc.toJson() + "}";
					processJson(jsonString);
//					MongoToMySQL.documentLabel.append(remoteCollections[colecaoID] + " has " + remoteCollection.countDocuments() + " objects\n");
//					System.out.println(remoteCollections[colecaoID] + " has " + remoteCollection.countDocuments() + " objects\n");
					count++;
					}
				
				//já percorreu todos os documentos existentes, refaz iterável e dorme x segundos do .ini
				try {
					System.out.println("All objects in " + remoteCollections[colecaoID] + " transfered with success");
					MongoToMySQL.documentLabel.append("All objects in " + remoteCollections[colecaoID] + " transfered with success");
					// reload cursor to account for new docs
					remoteDocCursor.close();
					remoteDocCursor = remoteCollection.find().skip(count).iterator();
					Thread.sleep(Integer.parseInt(sleep_frequency_in_milliseconds));
				} catch (InterruptedException e) {
					MongoToMySQL.documentLabel.append(e.toString());
					e.printStackTrace();
				}
			}
		}
	}
	
private void processJson(String json) {
		
		//como processar a mensagem que provem do MQTT e como a decompor de modo a inserir no MySQL

		JSONObject jsonObject = new JSONObject(json);
		JSONObject register = jsonObject.getJSONObject("doc");
		
		String zona = register.getString("Zona");
		String sensor = register.getString("Sensor");
		
		String timestamp = register.getString("Data");
		StringBuilder b = new StringBuilder();
		if(timestamp.contains("GMT")) {
			String[] a = timestamp.split(" ");
			b.append(a[0]);
			b.append(" ");
			b.append(a[2]);
		} else {
			String[] a = timestamp.split("T");
			b.append(a[0]);
			b.append(" ");
			String[] c = a[1].split("Z");
			b.append(c[0]);
			
		}
		double measure = register.getDouble("Medicao");
		
		String IDZona;
		if (zona.contentEquals("Z1")) {
			IDZona = "1";
		} else {
			IDZona = "2";
		}
		String Tipo = String.valueOf(sensor.charAt(0));
		
		try {

			int sensorID = 0;
			
			if(sensor.equals("H1")) {
				sensorID = 1;
			} else {
				if(sensor.equals("L1")) {
					sensorID = 2;
				} else {
					if(sensor.equals("T1")) {
						sensorID = 3;
					} else {
						if(sensor.equals("H2")) {
							sensorID = 4;
						} else { 
							if(sensor.equals("L2")) {
								sensorID = 5;
							} else {
								sensorID = 6;
							}
						}
					}
				}
			}
			
			int Zona2 = Integer.parseInt(IDZona);
			String query = "INSERT INTO medicao (Zona, Tipo, IDSensor, Hora, Leitura, valid) " + "VALUES ( '" + Zona2 + "', '" + Tipo + "', " + sensorID + ", '" + b.toString() + "', '" + measure + "', " + 1 + ")";
			Statement stmt = sqlConnection.createStatement();
//			System.out.println("Executing Query: " + query);
			stmt.executeUpdate(query);
			
			if (numberSent % 1000 == 0) {
				
				if(numberSent > 0) {
					long endtime = System.currentTimeMillis();
					double time = (endtime - timeCounter) /1000.00;
					System.out.println(numberSent + " objetos enviados");
					System.out.println("Últimos 1000 objetos enviados em " + time + " segundos");
					documentLabel.append(numberSent + " objetos enviados\n");
					documentLabel.append("Últimos 1000 objetos enviados em " + time + " segundos\n");
				}
				timeCounter = System.currentTimeMillis();
			}
			numberSent++;
//			MongoToMySQL.documentLabel.append(query);
//			MongoToMySQL.documentLabel.append("O objeto foi introduzido na base de dados MySQL!\n");

		} catch (SQLException ex) {

			System.out.println("Nao foi possivel introduzir o objeto na base de dados MySQL devido a perda de ligacao com a mesma!");
			MongoToMySQL.documentLabel.append(json + "\n");
			MongoToMySQL.documentLabel.append("Nao foi possivel introduzir o objeto na base de dados MySQL devido a perda de ligacao com a mesma!\n");
		}
	}
	
	
	
	private static void clearMySQLEntries() {
		//eliminar todas as entradas anteriores na tabela
		//para efeito de alteracao de medicoes ou limites dos sensores
		try {
			
			Statement stm = sqlConnection.createStatement();
			String query = "DELETE FROM medicao";
			System.out.println("Executing Query: " + query);
			stm.executeUpdate(query);
			MongoToMySQL.documentLabel.append(query + " executed!" + "\n");
			MongoToMySQL.documentLabel.append("Entradas anteriores eliminadas, MySQL pronto a receber medicoes!" + "\n");
			System.out.println("Entradas anteriores eliminadas, MySQL pronto a receber novas medicoes!");
			Statement alter = sqlConnection.createStatement();
			String query2 = "ALTER TABLE 'medicao' AUTO_INCREMENT=1";
			System.out.println("Executing Query: " + query2);
			alter.executeUpdate(query);
			MongoToMySQL.documentLabel.append(query2 + " executed!" + "\n");
			MongoToMySQL.documentLabel.append("Auto-incremento alterado, MySQL pronto a receber medicoes!" + "\n");
			System.out.println("Auto-incremento alterado, MySQL pronto a receber novas medicoes!");

		} catch (SQLException ex) {
		
			MongoToMySQL.documentLabel.append("Nao foi possivel limpar a tabela medicao!" + "\n");
			System.out.println("Nao foi possivel limpar a tabela medicao!");
		}
	}

	private static void connectToMySQL () {
		//driver necessario para estabelecer ligacao com MySQL
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		}
		catch (Exception e) {
			System.out.println("Nao foi possivel carregar o driver do mysql!");
			e.printStackTrace();
		}
		
		try {
			//conexao a base de dados MySQL
			String connectionString = "jdbc:mysql://localhost:3306/" + MongoToMySQL.server_local +  "?useSSL=false";
//			MongoToMySQL.user="root";
//			MongoToMySQL.password="";
			sqlConnection = DriverManager.getConnection(connectionString,
					MongoToMySQL.user, MongoToMySQL.password);
			System.out.println("Ligacao a MySQL " + " estabelecida!");
			documentLabel.append("Ligacao a MySQL " + " estabelecida!" + "\n");
			//createWindow();

		} catch (SQLException ex) {

			System.out.println("Nao foi possivel ligar ao servidor MySQL. Verifique as ligacoes e credenciais!");
			MongoToMySQL.documentLabel.append("Nao foi possivel ligar ao servidor MySQL!" + "\n");
			MongoToMySQL.documentLabel.append("Verifique as ligacoes e reinicie o programa!" + "\n");
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
}
