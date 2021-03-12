package SID2021.projetoSID;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.conversions.Bson;
import java.util.Date;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import java.text.SimpleDateFormat;
import org.eclipse.paho.client.mqttv3.MqttException;
import java.util.Random;
import javax.swing.JOptionPane;
import java.util.Properties;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import java.awt.Dimension;
import javax.swing.JLabel;
import javax.swing.JFrame;
import javax.swing.JTextArea;
import com.mongodb.DBCollection;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;

public class MongoToCloud implements MqttCallback {
	static MqttClient mqttclient;
	static DBCollection table;
	static String mongo_user;
	static String mongo_password;
	static String cloud_server;
	static String cloud_topic;
	static String mongo_replica;
	static String mongo_address;
	static String mongo_database;
	static String mongo_collection1;
	static String mongo_collection2;
	static String mongo_collection3;
	static String mongo_collection4;
	static String mongo_collection5;
	static String mongo_collection6;
	static String mongo_criteria;
	static String mongo_fieldquery;
	static String mongo_fieldvalue;
	static String delete_document;
	static String loop_query;
	static String display_documents;
	static String seconds_wait;
	static JTextArea documentLabel;
	static String mongo_authentication;
	public static String mongo_collection;
	static MongoDatabase database = null;

	private static void createWindow() {
		final JFrame frame = new JFrame("Mongo to Cloud");
		frame.setDefaultCloseOperation(3);
		final JLabel comp = new JLabel("Data from mongo: ", 0);
		comp.setPreferredSize(new Dimension(600, 30));
		final JScrollPane comp2 = new JScrollPane(MongoToCloud.documentLabel, 22, 32);
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

	public static void main(final String[] array) {
		
		createWindow();
		try {

			final Properties properties = new Properties();
			properties.load(MongoToCloud.class.getResourceAsStream("/MongoToCloud.ini"));

			MongoToCloud.mongo_address = properties.getProperty("mongo_address");
			MongoToCloud.mongo_user = properties.getProperty("mongo_user");
			MongoToCloud.mongo_password = properties.getProperty("mongo_password");
			MongoToCloud.mongo_database = properties.getProperty("mongo_database");
			MongoToCloud.mongo_collection1 = properties.getProperty("mongo_collection1");
			MongoToCloud.mongo_collection2 = properties.getProperty("mongo_collection2");
			MongoToCloud.mongo_collection3 = properties.getProperty("mongo_collection3");
			MongoToCloud.mongo_collection4 = properties.getProperty("mongo_collection4");
			MongoToCloud.mongo_collection5 = properties.getProperty("mongo_collection5");
			MongoToCloud.mongo_collection6 = properties.getProperty("mongo_collection6");
			MongoToCloud.mongo_fieldquery = properties.getProperty("mongo_fieldquery");
			MongoToCloud.mongo_fieldvalue = properties.getProperty("mongo_fieldvalue");
			MongoToCloud.delete_document = properties.getProperty("delete_document");
			MongoToCloud.display_documents = properties.getProperty("display_documents");
			MongoToCloud.mongo_authentication = properties.getProperty("mongo_authentication");
			MongoToCloud.mongo_replica = properties.getProperty("mongo_replica");
			MongoToCloud.loop_query = properties.getProperty("loop_query");
			MongoToCloud.seconds_wait = properties.getProperty("delay");
			MongoToCloud.cloud_server = properties.getProperty("cloud_server");
			MongoToCloud.cloud_topic = properties.getProperty("cloud_topic");

			Thread[] varias = new Thread[6];
			MongoToCloud contador = new MongoToCloud();
			new MongoToCloud().connecCloud();
			String str = new MongoToCloud().jsonToCloud();
			new MongoToCloud().createDatabase(str);
			
			for (int i = 0; i < 6; i++) {

				String atual = "mongo_collection";
				int j = i + 1;
				String n_colecao = String.valueOf(j);
				String atual2 = atual.concat(n_colecao);
				int id = i;
				varias[i] = contador.new incrementador(properties.getProperty(atual2), id);

			}
			for (Thread t : varias) {

				t.start();
			}
		} catch (Exception obj) {
			System.out.println("Error reading MongoToCloud.ini file " + obj);
			JOptionPane.showMessageDialog(null, "The MongoToCloud inifile wasn't found.", "Mongo To Cloud", 0);
		}

	}
	public void createDatabase(String str) {
		
		try {
		database = new MongoClient(new MongoClientURI(str)).getDatabase(MongoToCloud.mongo_database);
		MongoToCloud.documentLabel.append("Conexao ao servidor mongo estabelecida!\n");
		System.out.println("Conexao ao servidor mongo estabelecida!");
		
		} catch (Exception ex) {
			System.out.println("Nao foi possivel conectar ao servidor mongo!");
			MongoToCloud.documentLabel.append("Nao foi possivel conectar ao servidor mongo!\n");
		}
	}

	public class incrementador extends Thread {

		public String colecao;
		public int id;

		incrementador(String colecao, int id) {

			this.colecao = colecao;
			this.id = id;
		}

		public void run() {

			final MongoCollection collection = database.getCollection(colecao);
			atualizar(collection, database, colecao);
		}
	}

	protected String getSaltString() {
		final String s = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		final StringBuilder sb = new StringBuilder();
		final Random random = new Random();
		while (sb.length() < 18) {
			sb.append(s.charAt((int) (random.nextFloat() * s.length())));
		}
		return sb.toString();
	}

	public void connecCloud() {
		try {
			(MongoToCloud.mqttclient = new MqttClient(MongoToCloud.cloud_server,
					"MongoToCloud" + this.getSaltString() + MongoToCloud.cloud_topic)).connect();
			MongoToCloud.mqttclient.setCallback((MqttCallback) this);
			MongoToCloud.mqttclient.subscribe(MongoToCloud.cloud_topic);
			MongoToCloud.documentLabel.append("Conexao ao servidor broker estabelecida!\n");
			System.out.println("Conexao ao servidor broker estabelecida!");
		} catch (MqttException ex) {
			
			MongoToCloud.documentLabel.append("Nao foi possivel estabelecer ligacao ao servidor broker!\n");
			System.out.println("Nao foi possivel estabelecer ligacao ao servidor broker!");
		}
	}

	public String jsonToCloud() {
	
		String string = "mongodb://";
		if (MongoToCloud.mongo_authentication.equals("true")) {
			string = string + MongoToCloud.mongo_user + ":" + MongoToCloud.mongo_password + "@";
		}
		String str = string + MongoToCloud.mongo_address;
		if (!MongoToCloud.mongo_replica.equals("false")) {
			if (MongoToCloud.mongo_authentication.equals("true")) {
				str = str + "/?replicaSet=" + MongoToCloud.mongo_replica + "&authSource=admin";
			} else {
				str = str + "/?replicaSet=" + MongoToCloud.mongo_replica;
			}
		} else if (MongoToCloud.mongo_authentication.equals("true")) {
			str += "/?authSource=admin";
		}
		
		return str;

	}

	public synchronized void writeSensor(final String s) {
		try {
			final MqttMessage mqttMessage = new MqttMessage();
			mqttMessage.setPayload(s.getBytes());
			MongoToCloud.mqttclient.publish(MongoToCloud.cloud_topic, mqttMessage);
		} catch (MqttException ex) {
			ex.printStackTrace();
		}
	}

	public void connectionLost(final Throwable t) {
		
		MongoToCloud.documentLabel.append("A ligacao ao servidor broker foi interrompida!\n");
		System.out.println("A ligacao ao servidor broker foi interrompida!");
	}

	public synchronized void deliveryComplete(final IMqttDeliveryToken mqttDeliveryToken) {
		
		MongoToCloud.documentLabel.append("A entrega de mensagens foi concluida!\n");
		System.out.println("A entrega de mensagens foi concluida!");
	}

	public void messageArrived(final String s, final MqttMessage mqttMessage) {
	}

	static {
		MongoToCloud.mongo_user = new String();
		MongoToCloud.mongo_password = new String();
		MongoToCloud.cloud_server = new String();
		MongoToCloud.cloud_topic = new String();
		MongoToCloud.mongo_replica = new String();
		MongoToCloud.mongo_address = new String();
		MongoToCloud.mongo_database = new String();
		MongoToCloud.mongo_collection1 = new String();
		MongoToCloud.mongo_collection2 = new String();
		MongoToCloud.mongo_collection3 = new String();
		MongoToCloud.mongo_collection4 = new String();
		MongoToCloud.mongo_collection5 = new String();
		MongoToCloud.mongo_collection6 = new String();
		MongoToCloud.mongo_criteria = new String();
		MongoToCloud.mongo_fieldquery = new String();
		MongoToCloud.mongo_fieldvalue = new String();
		MongoToCloud.delete_document = new String();
		MongoToCloud.loop_query = new String();
		MongoToCloud.display_documents = new String();
		MongoToCloud.seconds_wait = new String();
		MongoToCloud.documentLabel = new JTextArea("\n");
		MongoToCloud.mongo_authentication = new String();
	}

	public void atualizar(MongoCollection colecao, MongoDatabase database, String coleta) {
		final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
		final Document document = new Document();
		int i = 0;
		int j = 0;
		int k = 0;
		while (i == 0) {

			final Date date = new Date(System.currentTimeMillis());

			synchronized (MongoToCloud.documentLabel) {
				MongoToCloud.documentLabel.append(simpleDateFormat.format(date) + "\n");
				MongoToCloud.documentLabel
						.append("Loop: " + j + " da colecao " + colecao.getNamespace() + " iniciado!\n");
			}
			final FindIterable find = colecao.find((Bson) document);

			find.iterator();

			final MongoCursor iterator = find.projection(Projections.excludeId()).iterator();
			while (iterator.hasNext()) {
				
				++k;
				final Document document3 = (Document) iterator.next();
				final String string2 = "{id:" + k + ", doc:" + document3.toJson() + "}";

				this.writeSensor(string2);
				if (MongoToCloud.display_documents.equals("true")) {
					
					synchronized(this) {
					MongoToCloud.documentLabel.append(string2 + "\n");
					System.out.println(string2);	
					}
					
				}
				if (!MongoToCloud.seconds_wait.equals("0")) {
					try {
						Thread.sleep(Integer.parseInt(MongoToCloud.seconds_wait));
					} catch (Exception ex) {
					}
				}
			}
			++j;
			if (MongoToCloud.delete_document.equals("true")) {
				if (!MongoToCloud.mongo_fieldquery.equals("null")) {
					colecao.deleteMany(
							Filters.eq(MongoToCloud.mongo_fieldquery, (Object) MongoToCloud.mongo_fieldvalue));
				}
				if (MongoToCloud.mongo_fieldquery.equals("null")) {
					database.getCollection(coleta).drop();
				}
			}
			if (!MongoToCloud.loop_query.equals("true")) {
				i = 1;
			}
			try {
			Thread.sleep(1000);
			} catch(Exception e) {
				
			}
		}
	}
}