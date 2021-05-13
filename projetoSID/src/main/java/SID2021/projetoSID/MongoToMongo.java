package SID2021.projetoSID;

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
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

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
	private static File myObj;
	static JPanel panel;
	   static JLabel user_label, password_label, user_label_remoto, password_label_remoto, message;
	   static JTextField userName_text_local;
	   static JPasswordField password_text_local;
	   static JTextField userName_text_remoto;
	   static JPasswordField password_text_remoto;
	   static JButton submit, cancel;
	   
	   static Thread[] threads;

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
		frame.addWindowListener(new java.awt.event.WindowAdapter() {
		    @Override
		    public void windowClosing(java.awt.event.WindowEvent windowEvent) {
		        
		    	System.out.println("O programa foi fechado pelo utilizador!");
				MongoToCloud.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					//System.out.println(threads.length);
					for(int i = 0; i < threads.length; i++) {
						
						threads[i].interrupt();
						//REVISAO
						System.out.println(((incrementador) threads[i]).getCount() + " stored for " + ((incrementador) threads[i]).getRemote()[i].toString());
						saveProgress(((incrementador) threads[i]).getRemote(), ((incrementador) threads[i]).getID(), ((incrementador) threads[i]).getCount());
					}
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
				MongoToCloud.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					
					for(int i = 0; i < threads.length; i++) {
						
						threads[i].interrupt();
						System.out.println(((incrementador) threads[i]).getCount() + " stored for " + ((incrementador) threads[i]).getRemote()[i].toString());
						saveProgress(((incrementador) threads[i]).getRemote(), ((incrementador) threads[i]).getID(), ((incrementador) threads[i]).getCount());
					}
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
				      myObj = new File("erros_de_ligacao_mongotomongo.txt");
				     
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
					properties.load(MongoToMongo.class.getResourceAsStream("/MongoToMongo.ini"));

					MongoToMongo.remote_mongo_address = properties.getProperty("remote_mongo_address");
					MongoToMongo.remote_mongo_authentication = properties.getProperty("remote_mongo_authentication");
					//MongoToMongo.remote_mongo_user = properties.getProperty("remote_mongo_user");
					//MongoToMongo.remote_mongo_password = properties.getProperty("remote_mongo_password");
					MongoToMongo.remote_db = properties.getProperty("remote_db");
					MongoToMongo.local_replica_set = properties.getProperty("local_replica_set");
					MongoToMongo.local_replica_name = properties.getProperty("local_replica_name");
					MongoToMongo.local_mongo_address = properties.getProperty("local_mongo_address");
					MongoToMongo.local_mongo_authentication = properties.getProperty("local_mongo_authentication");
					//MongoToMongo.local_mongo_user = properties.getProperty("local_mongo_user");
					//MongoToMongo.local_mongo_password = properties.getProperty("local_mongo_password");
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
					Date date = new Date();
				      Timestamp ts=new Timestamp(date.getTime());
				      try {
				    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				    	  appendLine.append("Ocorreu um erro ao ler o ficheiro MongoToMongo.ini " + ts + "\n");
				    	  appendLine.close();
				    	  System.exit(0);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

					threads = new Thread[numRemoteCollections];
					MongoToMongo mongoToMongo = new MongoToMongo();
					for (int i = 0; i < numRemoteCollections; i++) {
						threads[i] = mongoToMongo.new incrementador(i, MongoToMongo.remoteCollections);
						try {
						threads[i].start();
						} catch (Exception e) {
							MongoToMongo.documentLabel.append("Nao foi possivel iniciar a Thread " + i + "\n");
							System.out.println(e);
						}
					}
	}

	public static void main(String[] args) {
		
		final JFrame frame2 = new JFrame("Mongo To Mongo");
		// Username Label
	      user_label = new JLabel();
	      user_label.setText("Username do Mongo Local:");
	      userName_text_local = new JTextField();
	      // Password Label
	      password_label = new JLabel();
	      password_label.setText("Password do Mongo Local:");
	      password_text_local = new JPasswordField();
	      user_label_remoto = new JLabel();
	      user_label_remoto.setText("Username do Mongo Remoto:");
	      userName_text_remoto = new JTextField();
	      password_label_remoto = new JLabel();
	      password_label_remoto.setText("Password do Mongo Remoto:");
	      password_text_remoto = new JPasswordField();
	      
	      // Submit
	      submit = new JButton("SUBMIT");
	      panel = new JPanel(new GridLayout(5, 1));
	      panel.add(user_label);
	      panel.add(userName_text_local);
	      panel.add(password_label);
	      panel.add(password_text_local);
	      panel.add(user_label_remoto);
	      panel.add(userName_text_remoto);
	      panel.add(password_label_remoto);
	      panel.add(password_text_remoto);
	      message = new JLabel();
	      panel.add(message);
	      panel.add(submit);
	      frame2.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	      // Adding the listeners to components..
	      submit.addActionListener(new ActionListener() {
	    	  @Override
				public void actionPerformed(final ActionEvent actionEvent) {
	    	  MongoToMongo.local_mongo_user = userName_text_local.getText();
	          MongoToMongo.local_mongo_password = password_text_local.getText();
	          MongoToMongo.remote_mongo_user = userName_text_remoto.getText();
	          MongoToMongo.remote_mongo_password = password_text_remoto.getText();
	          comeca();
	    	  }
	      });
	      frame2.add(panel, BorderLayout.CENTER);
	      frame2.setTitle("Mongo To Mongo");
	      frame2.setLocationRelativeTo(null);
	      frame2.setSize(450,150);
	      frame2.setVisible(true);

	}

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

	private static void readLocalCollections(final Properties properties) {
		String collectionName = "something";
		for (int i = 0; i < numRemoteCollections; i++) {
			if (collectionName == null || collectionName.isEmpty()) {
				System.out.println("O numero de colecoes locais tem de ser o mesmo que as colecoes remotas!");
				JOptionPane.showMessageDialog(null, "O numero de colecoes locais tem de ser o mesmo que as colecoes remotas!");
				Date date = new Date();
			      Timestamp ts=new Timestamp(date.getTime());
			      try {
			    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
			    	  appendLine.append("O numero de colecoes locais tem de ser o mesmo que as colecoes remotas " + ts + "\n");
			    	  appendLine.close();
			    	  System.exit(0);
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
			}
			collectionName = properties.getProperty("local_collection" + Integer.toString(i + 1));
			localCollections[i] = collectionName;
		}
	}

	public class incrementador extends Thread {

		public int colecaoID;
		public String[] remoteCollections;
		public int count;

		incrementador(int colecaoID, String[] remoteCollections) {

			this.colecaoID = colecaoID;
			this.remoteCollections = remoteCollections;
		}

		public void run() {

			//new MongoToMongo();
			connectToMongos();
		}
		
		public int getCount() {
			
			return this.count;
		}
		
		public String[] getRemote() {
			
			return this.remoteCollections;
		}
		
		public int getID() {
			
			return this.colecaoID;
		}
		
		private void transferDocs(MongoDatabase remotedb, MongoDatabase localdb, String[] localCollections) {
			MongoCollection<Document> remoteCollection = remotedb.getCollection(remoteCollections[colecaoID]);
			MongoCollection<Document> localCollection = localdb.getCollection(localCollections[colecaoID]);
//			System.out.println(remoteCollection.countDocuments());
			int i = 0;
			this.count = 0;
			MongoCursor<Document> remoteDocCursor = remoteCollection.find().iterator();

			if (load_progress.equals("true")) {
				remoteDocCursor = loadProgressFromFile(remoteCollections, colecaoID, remoteCollection, remoteDocCursor);
				try {
					File file = new File(remote_db + "_" + remoteCollections[colecaoID] + "_" + local_db + localCollections[colecaoID] + "_savefile.txt");
					if (file.exists()) {
						BufferedReader reader = new BufferedReader(new FileReader(file));
						String currentLine;

						while ((currentLine = reader.readLine()) != null) {
							if (currentLine.contains(remote_db + ";" + remoteCollections[colecaoID])) {
								String[] split = currentLine.split(";");
								count = Integer.parseInt(split[2]);
							}
						}
						reader.close();

					}
				} catch (IOException e) {
					MongoToMongo.documentLabel
							.append("Não foi possível carregar o progresso para " + remoteCollections[colecaoID] + "\n");
					e.printStackTrace();
					Date date = new Date();
				      Timestamp ts=new Timestamp(date.getTime());
				      try {
				    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				    	  appendLine.append("Nao foi possivel carregar o progresso para " + remoteCollections[colecaoID] + " " + ts + "\n");
				    	  appendLine.close();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			try {
			while (i == 0) {
				while (remoteDocCursor.hasNext()) {
					Document remoteDoc = remoteDocCursor.next();
					// Verificar se já tem doc na db local
					BasicDBObject queryDuplicates = new BasicDBObject();
					queryDuplicates.put("_id", remoteDoc.getObjectId("_id"));
					long duplicates = localCollection.count(queryDuplicates);
					if (duplicates == 0) {
						localCollection.insertOne(remoteDoc);
//						System.out.println("Inserted object with id " + remoteDoc.getObjectId("_id").toString() + " into "
//								+ localCollections[colecaoID]);
						if (count % 100 == 0) {
							// saves progress every 100 documents
							saveProgress(remoteCollections, colecaoID, count);
							MongoToMongo.documentLabel.append(localCollections[colecaoID] + " has "
									+ localCollection.countDocuments() + " objects\n");
						}
					}
					count++;
				}
				try {
					System.out.println("All objects in " + remoteCollections[colecaoID] + " transfered with success");
					// reload cursor to account for new docs
					remoteDocCursor.close();
					remoteDocCursor = loadProgressFromFile(remoteCollections, colecaoID, remoteCollection, remoteDocCursor);
					Thread.sleep(Integer.parseInt(sleep_frequency_in_milliseconds));
				} catch (InterruptedException e) {
					MongoToMongo.documentLabel.append(e.toString());
					e.printStackTrace();
				}
			}
			} catch (Exception ex) {
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		        System.out.println("Nao foi possivel ligar ao mongo local ou a ligacao foi interrompida!");
		        MongoToMongo.documentLabel.append("Nao foi possivel ligar ao mongo local ou a ligacao foi interrompida!" + "\n");
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("Nao foi possivel ligar ao mongo local ou a ligacao foi interrompida " + ts + "\n");
					appendLine.close();
					//saveProgress(remoteCollections, colecaoID, count);
					System.exit(0);
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		
		private void connectToMongos() {
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
						localClient = MongoClients.create(connectionStringLocal);
					}
					MongoToMongo.documentLabel.append("Thread " + Integer.toString(colecaoID + 1) + " connected to remote "
							+ remoteCollections[colecaoID] + " and to local " + localCollections[colecaoID] + "\n");
			//printDBNames(localClient); printDBNames(remoteClient);
					MongoDatabase remotedb = remoteClient.getDatabase(remote_db);
					MongoDatabase localdb = localClient.getDatabase(local_db);
			
					// apagar documentos de coleções locais antes de iniciar a transferência
					if (clear_local_collections_before_start.equals("true")) {
						deleteDocsFromLocalCollection(localdb, colecaoID);
					}

					transferDocs(remotedb, localdb, localCollections);
				} catch (Exception e) {
					MongoToMongo.documentLabel.append("Nao foi possivel conectar ao servidor mongo remoto!\n");
					Date date = new Date();
				      Timestamp ts=new Timestamp(date.getTime());
				      try {
				    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				    	  appendLine.append("Nao foi possivel conectar ao servidor mongo remoto. Verifique a ligacao e credenciais " + ts + "\n");
				    	  appendLine.close();
				    	  System.exit(0);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			} catch (Exception e) {
				MongoToMongo.documentLabel.append("Nao foi possivel conectar ao servidor mongo remoto!\n");
				Date date = new Date();
			      Timestamp ts=new Timestamp(date.getTime());
			      try {
			    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
			    	  appendLine.append("Nao foi possivel conectar ao servidor mongo remoto. Verifique a ligacao e credenciais " + ts + "\n");
			    	  appendLine.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

		}
		
	}

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

	private static MongoCursor<Document> loadProgressFromFile(String[] remoteCollections, int colecaoID,
			MongoCollection<Document> remoteCollection, MongoCursor<Document> remoteDocCursor) {
		try {
			File file = new File(remote_db + "_" + remoteCollections[colecaoID] + "_" + local_db + localCollections[colecaoID] + "_savefile.txt");
			if (file.exists()) {
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String currentLine;

				while ((currentLine = reader.readLine()) != null) {
					if (currentLine.contains(remote_db + ";" + remoteCollections[colecaoID])) {
						String[] split = currentLine.split(";");
						remoteDocCursor = remoteCollection.find().skip(Integer.parseInt(split[2].trim())).iterator();
						System.out.println("skipped");
						MongoToMongo.documentLabel.append("Progress loaded for " + remoteCollections[colecaoID] + "\n");
					}
				}
				reader.close();

			}
		} catch (IOException e) {
			MongoToMongo.documentLabel
					.append("Não foi possível carregar o progresso para " + remoteCollections[colecaoID] + "\n");
			e.printStackTrace();
			Date date = new Date();
		      Timestamp ts=new Timestamp(date.getTime());
		      try {
		    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
		    	  appendLine.append("Nao foi possivel carregar o progresso para " + remoteCollections[colecaoID] + " " + ts + "\n");
		    	  appendLine.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return remoteDocCursor;
	}

	private static void saveProgress(String[] remoteCollections, int colecaoID, int count) {
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
								+ Integer.toString(count - 1) + "\n");
					}
					exists = true;
				}
			}
			if (exists.equals(false)) {
				appendLine.append(
						remote_db + ";" + remoteCollections[colecaoID] + ";" + Integer.toString(count - 1) + "\n");
			}
			removeLine.close();
			appendLine.close();
			reader.close();
		} catch (IOException e) {
			MongoToMongo.documentLabel
					.append("Nao foi possivel guardar o progresso para " + remoteCollections[colecaoID] + "\n");
			e.printStackTrace();
			Date date = new Date();
		      Timestamp ts=new Timestamp(date.getTime());
		      try {
		    	  BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
		    	  appendLine.append("Nao foi possivel guardar o progresso para " + remoteCollections[colecaoID] + " " + ts + "\n");
		    	  appendLine.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	private static void deleteDocsFromLocalCollection(MongoDatabase localdb, int colecaoID) {
		BasicDBObject query = new BasicDBObject();
		MongoCollection<Document> localCollection = localdb.getCollection(localCollections[colecaoID]);
		localCollection.deleteMany(query);
		System.out.println("Coleção local " + localCollections[colecaoID] + " limpa");
		MongoToMongo.documentLabel.append("Coleção local " + localCollections[colecaoID] + " limpa\n");
	}

}
