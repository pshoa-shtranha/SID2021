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
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JOptionPane;
import java.util.Properties;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;

import javax.swing.JButton;
import javax.swing.JScrollPane;
import java.awt.Dimension;
import javax.swing.JLabel;
import javax.swing.JFrame;
import javax.swing.JTextArea;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

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
	static String display_documents;
	static String seconds_wait;
	static JTextArea documentLabel;
	static String mongo_authentication;
	public static String mongo_collection;
	static MongoDatabase database = null;
	static String clear_local_collections_before_start;
	static String sleep_frequency_in_milliseconds;
	static String load_progress;
	static String frequency_of_load;
	private static File myObj;
	static final Properties properties = new Properties();
	//static boolean start = false;
	static boolean terminated = false;
	static String progress_saving;
	static Thread[] varias;
	
	private static void createWindow() {
		
		//criacao de janela
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
				terminated = true;
				System.out.println("O programa foi fechado pelo utilizador!");
				MongoToCloud.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					
					
					//REVISAO
					/*for(int i = 0; i < varias.length; i++) {
						
						varias[i].interrupt();
					}*/
					for(int i = 0; i < varias.length; i++) {
						
							//eliminar as medicoes ja transferidas entretanto
						int x = (((incrementador) varias[i]).getCount() + 1) % Integer.parseInt(MongoToCloud.progress_saving);
						//((incrementador) MongoToCloud.varias[i]).getCollection().deleteMany(Filters.lt("_id", ((incrementador) varias[i]).getDocument().get("_id")));
							System.out.println(x + " medicoes foram eliminadas da colecao!");
						
						saveProgress(database, ((incrementador) varias[i]).getCollection(), ((incrementador) MongoToCloud.varias[i]).getColecao(), ((incrementador) varias[i]).getCount());
					}
					
					
					
					System.exit(0);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

	public static void main(final String[] array) {
		
		try {
		      myObj = new File("erros_de_ligacao_mongotocloud.txt");
		     
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
		
		createWindow();
		try {

			//obter as configuracoes do ficheiro ini
			//Properties properties = new Properties();
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
			MongoToCloud.display_documents = properties.getProperty("display_documents");
			MongoToCloud.mongo_authentication = properties.getProperty("mongo_authentication");
			MongoToCloud.mongo_replica = properties.getProperty("mongo_replica");
			//MongoToCloud.seconds_wait = properties.getProperty("delay");
			MongoToCloud.cloud_server = properties.getProperty("cloud_server");
			MongoToCloud.cloud_topic = properties.getProperty("cloud_topic");
			MongoToCloud.sleep_frequency_in_milliseconds = properties.getProperty("sleep_frequency_in_milliseconds");
			MongoToCloud.load_progress = properties.getProperty("load_progress");
			MongoToCloud.progress_saving = properties.getProperty("progress_saving");

			//obter os nomes das diferentes colecoes e abrir uma thread para cada uma delas
			varias = new Thread[6];
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
				
				varias[i] = contador.new incrementador(properties.getProperty(atual2), id, -1);

			}
			for (Thread t : varias) {

				t.start();
			}
		} catch (Exception obj) {
			System.out.println("Error reading MongoToCloud.ini file " + obj);
			JOptionPane.showMessageDialog(null, "The MongoToCloud inifile wasn't found.", "Mongo To Cloud", 0);
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Problema ao ler o ficheiro MongoToCloud.ini " + ts + "\n");
				appendLine.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	public void createDatabase(String str) {
		
		//conexao ao mongo
		try {
		database = new MongoClient(new MongoClientURI(str)).getDatabase(MongoToCloud.mongo_database);
		MongoToCloud.documentLabel.append("Conexao ao servidor mongo estabelecida!\n");
		System.out.println("Conexao ao servidor mongo estabelecida!");
		
		} catch (Exception ex) {
			System.out.println("Nao foi possivel conectar ao servidor mongo!");
			MongoToCloud.documentLabel.append("Nao foi possivel conectar ao servidor mongo!\n");
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel conectar ao servidor mongo " + ts + "\n");
				appendLine.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	public class incrementador extends Thread {

		//classe para as threads
		public String colecao;
		public int id;
		public int count;
		public MongoCollection collection;
		public Document document3;

		incrementador(String colecao, int id, int count) {

			this.colecao = colecao;
			this.id = id;
			this.count = count;
			this.document3 = null;
		}
		
		public int getCount() {
			
			return this.count;
		}
		
		public String getColecao() {
			
			return this.colecao;
		}
		
		public MongoCollection getCollection() {
			
			return this.collection;
		}

		public Document getDocument() {
			
			return this.document3;
		}
		
		public void run() {

			collection = database.getCollection(colecao);
			atualizar();
		}
		
		public void writeSensor(final String s) {
			
			//classe para enviar as mensagens para o servidor
			try {
				final MqttMessage mqttMessage = new MqttMessage();
				mqttMessage.setPayload(s.getBytes());
				MongoToCloud.mqttclient.publish(MongoToCloud.cloud_topic, mqttMessage);
			} catch (MqttException ex) {
				ex.printStackTrace();
			}
		}
		
		public void atualizar() {
			
			
			final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
			final Document document = new Document();
			int i = 0;
			int j = 0;
			
			if(MongoToCloud.load_progress.equals("false")) {
				saveProgress(database, collection, colecao, -1);
			}
			//int count = -1;
			
			//para nao dar erro caso haja menos documentos na colecao ao iniciar a aplicacao
			//se for este o caso, comeca a enviar os objetos a partir do inicio
			int numberDocumentsUpdated = -1;
		
			
			MongoCursor<Document> find = collection.find().iterator();
			try {
				File file = new File(colecao + "_savefile.txt");
				if (file.exists()) {
					BufferedReader reader = new BufferedReader(new FileReader(file));
					String currentLine;

					while ((currentLine = reader.readLine()) != null) {
						if (currentLine.contains(colecao + ";")) {
							String[] split = currentLine.split(";");
							numberDocumentsUpdated = Integer.parseInt(split[1].trim());
							//System.out.println(numberDocumentsUpdated);
						}
					}
					reader.close();

				}
			} catch (IOException e) {
				MongoToCloud.documentLabel
						.append("Não foi possível carregar o progresso a partir do ficheiro!\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("Nao foi possivel carregar o progresso a partir do ficheiro " + ts + "\n");
					appendLine.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			
			//REVISAO
			if(MongoToCloud.load_progress.equals("true")) {
				
				count = numberDocumentsUpdated;
				int q = count + 1;
				System.out.println(q + " documents loaded for " + colecao);
			}
			
			
			
			//ciclo infinito para estar sempre ha procura de novos documentos
			//sleep_frequency_in_milliseconds utilizado para evitar problema de espera ativa
			try {
			while (i == 0) {

				final Date date = new Date(System.currentTimeMillis());

				synchronized (MongoToCloud.documentLabel) {
					MongoToCloud.documentLabel.append(simpleDateFormat.format(date) + "\n");
					MongoToCloud.documentLabel
							.append("Loop: " + j + " da colecao " + collection.getNamespace() + " iniciado!\n");
				}
				
				//cada vez que o iteravel tem um documento a seguir, enviar o mesmo para o MQTT
				while (find.hasNext()) {
					
					count = count + 1;
					document3 = (Document) find.next();
					final String string2 = "{id:" + count + ", doc:" + document3.toJson() + "}";
					Thread.sleep(20);
					this.writeSensor(string2);
					
					//caso o administrador pretenda que os documentos enviados aparecam no ecra
					if (MongoToCloud.display_documents.equals("true")) {
						
						synchronized(this) {
						MongoToCloud.documentLabel.append(string2 + "\n");
						System.out.println(string2);	
						}
						
					}
					
					//REVISAO
					if ((count + 1) % Integer.parseInt(MongoToCloud.progress_saving) == 0 && count > 0) {
						
						saveProgress(database, collection, colecao, count);
							//eliminar as 30 primeiras medicoes na colecao
						//collection.deleteMany(Filters.lt("_id", document3.get("_id")));
						System.out.println(MongoToCloud.progress_saving + " medicoes foram eliminadas da colecao " + colecao);
						break;
						
					}
					
				}
				//incrementacao no loop
				++j;
				Thread.sleep(Integer.parseInt(sleep_frequency_in_milliseconds));
				//caso o administrador pretenda eliminar os documentos depois de enviados
				//meter dentro do inner loop e fazer para eliminar de x em x
				
					find.close();
					find = collection.find().iterator();
				
			
			}
			} catch (Exception w) {
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		        System.out.println("A ligacao com o mongo foi interrompida!");
		        MongoToCloud.documentLabel.append("A ligacao com o mongo foi interrompida!" + "\n");
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("A ligacao com o mongo foi interrompida " + ts + "\n");
					appendLine.close();
					System.exit(0);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
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
		
		//conexao ao servidor MQTT
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
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel estabelecer ligacao ao servidor broker " + ts + "\n");
				appendLine.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	public String jsonToCloud() {
	
		//obter string de conexao conforme o tipo de autenticacao e se e replicaset ou nao
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

	public void connectionLost(final Throwable t) {
		
		//o que fazer quando a ligacao ao servidor e perdida
		MongoToCloud.documentLabel.append("A ligacao ao servidor broker foi interrompida!\n");
		System.out.println("A ligacao ao servidor broker foi interrompida!");
		Date date = new Date();
        Timestamp ts=new Timestamp(date.getTime());
       
        try {
        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
			appendLine.append("A ligacao ao servidor broker foi interrompida " + ts + "\n");
			appendLine.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public synchronized void deliveryComplete(final IMqttDeliveryToken mqttDeliveryToken) {
		
		//o que fazer quando a tranferencia de mensagens e concluida
		MongoToCloud.documentLabel.append("A transferencia da mensagem foi concluida!\n");
		System.out.println("A tranferencia da mensagem foi concluida!");
	}

	public void messageArrived(final String s, final MqttMessage mqttMessage) {
		
		//parar o programa quando o programa do pc2 para ou existe um problema de ligacao com o mysql
		
		if(mqttMessage.toString().startsWith("STOP") && !terminated) {
			if(mqttMessage.toString().equals("STOP")) {
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("O programa terminou devido ao fecho do programa do pc2 ou de um problema de ligacao com o mysql " + ts + "\n");
				System.out.println("O programa terminou devido ao fecho do programa do pc2 ou de um problema de ligacao com o mysql!");
				appendLine.close();
				
					//interromper as threads
				/*for(int i = 0; i < varias.length; i++) {
					
					varias[i].interrupt();
				}*/
					for(int i = 0; i < varias.length; i++) {
						
					//eliminar as medicoes ja transferidas entretanto
						int x = (((incrementador) varias[i]).getCount() + 1) % Integer.parseInt(MongoToCloud.progress_saving);
						//((incrementador) MongoToCloud.varias[i]).getCollection().deleteMany(Filters.lt("_id", ((incrementador) varias[i]).getDocument().get("_id")));
						System.out.println(x + " medicoes foram eliminadas da colecao!");
				
						saveProgress(database, ((incrementador) varias[i]).getCollection(), ((incrementador) MongoToCloud.varias[i]).getColecao(), ((incrementador) varias[i]).getCount());
					}
						
				System.exit(0);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			//System.exit(0);
			} else {
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("E necessario iniciar primeiro o programa CloudToMySQL no PC2 " + ts + "\n");
					System.out.println("E necessario iniciar primeiro o programa CloudToMySQL no PC2!");
					appendLine.close();
					System.exit(0);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		} 
		//}
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
		MongoToCloud.display_documents = new String();
		//MongoToCloud.seconds_wait = new String();
		MongoToCloud.documentLabel = new JTextArea("\n");
		MongoToCloud.mongo_authentication = new String();
		MongoToCloud.clear_local_collections_before_start = new String();
		MongoToCloud.sleep_frequency_in_milliseconds = new String();
		MongoToCloud.load_progress = new String();
		MongoToCloud.progress_saving = new String();
		
	}
	
	private static void saveProgress(MongoDatabase database, MongoCollection colecao, String coleta, int count) {
		
		//metodo utilizado para guardar o progresso nos ficheiros
		try {
			File file = new File(coleta + "_savefile.txt");
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
				if (currentLine.contains(coleta + ";")) {
					// faz com que se apagar a linha guardada tem que obrigatoriamente criar uma
					// linha nova antes de se poder parar o programa
					synchronized (MongoToCloud.class) {
						removeLine.write(currentLine + System.getProperty("line.separator"));
						appendLine.append(coleta + ";" + Integer.toString(count) + "\n");
					}
					exists = true;
				}
			}
			if (exists.equals(false)) {
				appendLine.append(
						coleta + ";" + Integer.toString(count) + "\n");
			}
			removeLine.close();
			appendLine.close();
			reader.close();
		} catch (IOException e) {
			MongoToCloud.documentLabel
					.append("Nao foi possivel guardar o progresso para o ficheiro!\n");
			e.printStackTrace();
		}
	}
}