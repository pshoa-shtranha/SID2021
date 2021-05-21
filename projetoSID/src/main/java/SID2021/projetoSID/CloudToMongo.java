package SID2021.projetoSID;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import lalss3.CloudToMySQL;
import lalss3.MongoToCloud.incrementador;
import twitter4j.JSONObject;

public class CloudToMongo implements MqttCallback{
	
	static MqttClient mqttclient;
	static String cloud_server;
	static String cloud_topic;
	
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
	static MongoDatabase localdb;
	static String connectionStringRemote;
	static String connectionStringLocal;
	static String[] localCollections = new String[1000];
	static int numLocalCollections;
	private static File myObj;
	static JPanel panel;
	   static JLabel user_label, password_label, message;
	   static JTextField userName_text_local;
	   static JPasswordField password_text_local;
	   static JButton submit, cancel;
	   
	   static Thread[] threads;

	private static void createWindow() {
		documentLabel = new JTextArea("\n");
		final JFrame frame = new JFrame("Cloud to Mongo");
		frame.setDefaultCloseOperation(3);
		final JLabel comp = new JLabel("Data from MQTT: ", 0);
		comp.setPreferredSize(new Dimension(600, 30));
		final JScrollPane comp2 = new JScrollPane(CloudToMongo.documentLabel, 22, 32);
		comp2.setPreferredSize(new Dimension(600, 200));
		final JButton comp3 = new JButton("Stop the program");
		frame.getContentPane().add(comp, "First");
		frame.getContentPane().add(comp2, "Center");
		frame.getContentPane().add(comp3, "Last");
		frame.setLocationRelativeTo(null);
		frame.pack();
		frame.setVisible(true);
		frame.addWindowListener(new java.awt.event.WindowAdapter() {
		    @Override
		    public void windowClosing(java.awt.event.WindowEvent windowEvent) {
		        
		    	System.out.println("O programa foi fechado pelo utilizador!");
				CloudToMongo.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					
				System.exit(0);
		        } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		});
		comp3.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent actionEvent) {
				System.out.println("O programa foi fechado pelo utilizador!");
				CloudToMongo.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					
				System.exit(0);
		        } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}
	
	public static void comeca() {
		// Desativar mensagens vermelhas irritantes
				try {
				      myObj = new File("erros_de_ligacao_cloudtomongo.txt");
				     
				      if (myObj.createNewFile()) {
				        System.out.println("File created: " + myObj.getName());
				       
				      } else {
				        System.out.println("File erros_de_ligacao already exists!");
				        
				      }
				      
				    } catch (IOException e) {
				      System.out.println("Ocorreu um erro ao criar erros_de_ligacao");
				      Date date = new Date();
				      Timestamp ts=new Timestamp(date.getTime());
				      try {
				    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				    	  appendLine.append("Ocorreu um erro ao criar erros_de_ligacao " + ts + "\n");
				    	  appendLine.close();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				    }
				Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
				createWindow();
				try {
					final Properties properties = new Properties();
					properties.load(CloudToMongo.class.getResourceAsStream("/CloudToMongo.ini"));

					CloudToMongo.local_replica_set = properties.getProperty("local_replica_set");
					CloudToMongo.local_replica_name = properties.getProperty("local_replica_name");
					CloudToMongo.local_mongo_address = properties.getProperty("local_mongo_address");
					CloudToMongo.local_mongo_authentication = properties.getProperty("local_mongo_authentication");
					CloudToMongo.local_db = properties.getProperty("local_db");
					CloudToMongo.clear_local_collections_before_start = properties
							.getProperty("clear_local_collections_before_start");
					CloudToMongo.sleep_frequency_in_milliseconds = properties.getProperty("sleep_frequency_in_milliseconds");
					CloudToMongo.load_progress = properties.getProperty("load_progress");
					CloudToMongo.cloud_server = properties.getProperty("cloud_server");
					CloudToMongo.cloud_topic = properties.getProperty("cloud_topic");
					readLocalCollections(properties);
					connectToMongo();
					
				} catch (Exception e1) {
					System.out.println("Error reading CloudToMongo.ini file " + e1);
					JOptionPane.showMessageDialog(null, "The CloudToMongo inifile wasn't found.", "Cloud To Mongo", 0);
					Date date = new Date();
				      Timestamp ts=new Timestamp(date.getTime());
				      try {
				    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				    	  appendLine.append("Ocorreu um erro ao ler o ficheiro CloudToMongo.ini " + ts + "\n");
				    	  appendLine.close();
				    	  System.exit(0);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

	}

	public static void main(String[] args) {
		
		final JFrame frame2 = new JFrame("Cloud To Mongo");
		// Username Label
	      user_label = new JLabel();
	      user_label.setText("Username do Mongo Local:");
	      userName_text_local = new JTextField();
	      // Password Label
	      password_label = new JLabel();
	      password_label.setText("Password do Mongo Local:");
	      password_text_local = new JPasswordField();
	      
	      // Submit
	      submit = new JButton("SUBMIT");
	      panel = new JPanel(new GridLayout(3, 1));
	      panel.add(user_label);
	      panel.add(userName_text_local);
	      panel.add(password_label);
	      panel.add(password_text_local);
	      message = new JLabel();
	      panel.add(message);
	      panel.add(submit);
	      frame2.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	      // Adding the listeners to components..
	      submit.addActionListener(new ActionListener() {
	    	  @Override
				public void actionPerformed(final ActionEvent actionEvent) {
	    	  CloudToMongo.local_mongo_user = userName_text_local.getText();
	          CloudToMongo.local_mongo_password = password_text_local.getText();
	          comeca();
	          CloudToMongo ctm = new CloudToMongo();
	          ctm.connecCloud();
	    	  }
	      });
	      frame2.add(panel, BorderLayout.CENTER);
	      frame2.setTitle("Cloud To Mongo");
	      frame2.setLocationRelativeTo(null);
	      frame2.setSize(450,150);
	      frame2.setVisible(true);

	}
	
	public void connecCloud() {
		
		//conexao ao servidor MQTT
		try {
			(this.mqttclient = new MqttClient(CloudToMongo.cloud_server,
					"CloudToMongo" + String.valueOf(new Random().nextInt(100000)) + "_" + CloudToMongo.cloud_topic))
							.connect();
			this.mqttclient.setCallback((MqttCallback) this);
			this.mqttclient.subscribe(CloudToMongo.cloud_topic);
			CloudToMongo.documentLabel.append("Ligacao ao servidor broker estabelecida!" + "\n");
			System.out.println("Ligacao ao servidor broker estabelecida!");
		} catch (MqttException ex) {
			ex.printStackTrace();
			System.out.println("Nao foi possivel ligar ao broker. Reinicie o programa!");
			CloudToMongo.documentLabel.append("Nao foi possivel ligar ao broker. Reinicie o programa!" + "\n");
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel ligar ao broker " + ts + "\n");
				appendLine.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	private static void readLocalCollections(final Properties properties) {
		String collectionName = "something";
		numLocalCollections = 0;
		collectionName = properties.getProperty("local_collection" + Integer.toString(numLocalCollections + 1));
		while (collectionName != null) {
			localCollections[numLocalCollections] = collectionName;
			numLocalCollections++;
			collectionName = properties.getProperty("local_collection" + Integer.toString(numLocalCollections + 1));
		}
	}

	

		
	private static void connectToMongo() {
			
			makeLocalConnectionString();

				try {
					MongoClient localClient;
					synchronized (CloudToMongo.class) {
						localClient = MongoClients.create(connectionStringLocal);
					}
			
					localdb = localClient.getDatabase(local_db);

				} catch (Exception e) {
					CloudToMongo.documentLabel.append("Nao foi possivel conectar ao servidor mongo local!\n");
					Date date = new Date();
				      Timestamp ts=new Timestamp(date.getTime());
				      try {
				    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				    	  appendLine.append("Nao foi possivel conectar ao servidor mongo local. Verifique a ligacao e credenciais " + ts + "\n");
				    	  appendLine.close();
				    	  System.exit(0);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
				if(clear_local_collections_before_start.equals("true")) {
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
				}

		}


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

	@Override
	public void connectionLost(Throwable cause) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		// TODO Auto-generated method stub
		processJson(message);
	}

	private void processJson(MqttMessage message) {
		
		//como processar a mensagem que provem do MQTT e como a decompor de modo a inserir no mongo local
		String json = message.toString();
		System.out.println(json);
		CloudToMongo.documentLabel.append(json + "\n");
		JSONObject jsonObject = new JSONObject(json);
		
		Document a = Document.parse(jsonObject.toString());
		
		String sensor = jsonObject.getString("Sensor");
		String sensor2 = sensor.toLowerCase();
		
		for(int i = 0; i < numLocalCollections; i++) {
			
			if(localCollections[i].contains(sensor2)) {
				MongoCollection<Document> localCollection = localdb.getCollection(localCollections[i]);
				
				localCollection.insertOne(a);
				break;
			}
		}

	}
	
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
		
	}

}
