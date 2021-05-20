package SID2021.projetoSID;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import twitter4j.JSONObject;

import org.eclipse.paho.client.mqttv3.MqttException;
import java.util.Random;

import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;

import java.util.Properties;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JScrollPane;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;

import javax.swing.JLabel;
import javax.swing.JFrame;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import java.sql.*;
import java.sql.Timestamp;
import java.util.Date;

public class CloudToMySQL implements MqttCallback {

	static MqttClient mqttclient;
	static String cloud_server;
	static String cloud_topic;
	static String user;
	static String password;
	static JTextArea documentLabel;
	static Connection connect = null;
	static int numberSent;
	static String delete_many;
	static String server_local;
	private static File myObj;
	static boolean terminated = false;
	static JPanel panel;
	   static JLabel user_label, password_label, message;
	   static JTextField userName_text;
	   static JPasswordField password_text;
	   static JButton submit, cancel;

	private static void createWindow() {

		//criar janela
		final JFrame frame = new JFrame("Cloud To MySQL");
		frame.setDefaultCloseOperation(3);
		final JLabel comp = new JLabel("Data from broker: ", 0);
		comp.setPreferredSize(new Dimension(600, 30));
		final JScrollPane scrollPane = new JScrollPane(CloudToMySQL.documentLabel, 22, 32);
		scrollPane.setPreferredSize(new Dimension(600, 200));
		final JButton comp2 = new JButton("Stop the program");
		frame.getContentPane().add(comp, "First");
		frame.getContentPane().add(scrollPane, "Center");
		frame.getContentPane().add(comp2, "Last");
		frame.setLocationRelativeTo(null);
		frame.pack();
		frame.setVisible(true);
		frame.addWindowListener(new java.awt.event.WindowAdapter() {
		    @Override
		    public void windowClosing(java.awt.event.WindowEvent windowEvent) {
		        
		    	terminated = true;
				System.out.println("O programa foi fechado pelo utilizador!");
				CloudToMySQL.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					try {
						final MqttMessage mqttMessage = new MqttMessage();
						String s = "STOP";
						mqttMessage.setPayload(s.getBytes());
						CloudToMySQL.mqttclient.publish(CloudToMySQL.cloud_topic, mqttMessage);
						
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (MqttException e) {
						e.printStackTrace();
					}
					System.exit(0);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		});
		comp2.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent actionEvent) {
				terminated = true;
				System.out.println("O programa foi fechado pelo utilizador!");
				CloudToMySQL.documentLabel.append("O programa foi fechado pelo utilizador!" + "\n");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("O programa foi fechado pelo utilizador " + ts + "\n");
					appendLine.close();
					try {
						final MqttMessage mqttMessage = new MqttMessage();
						String s = "STOP";
						mqttMessage.setPayload(s.getBytes());
						CloudToMySQL.mqttclient.publish(CloudToMySQL.cloud_topic, mqttMessage);
						
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} catch (MqttException e) {
						e.printStackTrace();
					}
					System.exit(0);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}
	
	public static void firstWindow() {
		
		final JFrame frame2 = new JFrame("Cloud To MySQL");
		// Username Label
	      user_label = new JLabel();
	      user_label.setText("Username do MySQL:");
	      userName_text = new JTextField();
	      // Password Label
	      password_label = new JLabel();
	      password_label.setText("Password:");
	      password_text = new JPasswordField();
	      // Submit
	      submit = new JButton("SUBMIT");
	      panel = new JPanel(new GridLayout(3, 1));
	      panel.add(user_label);
	      panel.add(userName_text);
	      panel.add(password_label);
	      panel.add(password_text);
	      message = new JLabel();
	      panel.add(message);
	      panel.add(submit);
	      frame2.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	      // Adding the listeners to components..
	      submit.addActionListener(new ActionListener() {
	    	  @Override
				public void actionPerformed(final ActionEvent actionEvent) {
	    	  CloudToMySQL.user = userName_text.getText();
	          CloudToMySQL.password = password_text.getText();
	          comeca();
	    	  }
	      });
	      frame2.add(panel, BorderLayout.CENTER);
	      frame2.setTitle("Cloud To MySQL");
	      frame2.setLocationRelativeTo(null);
	      frame2.setSize(450,150);
	      frame2.setVisible(true);
	}
	public static void comeca() {
		
		try {
		      myObj = new File("erros_de_ligacao_cloudtomysql.txt");
		     
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
		  
		//driver necessario para estabelecer ligacao com MySQL
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			
		} catch (Exception e) {
			System.out.println("Nao foi possivel carregar o driver do mysql!");
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel carregar o driver do mysql " + ts + "\n");
				appendLine.close();
				
				System.exit(0);
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		try {
			
			//todas as configuracoes introduzidas no ficheiro ini
			final Properties properties = new Properties();
			properties.load(CloudToMySQL.class.getResourceAsStream("/CloudToMySQL.ini"));
			CloudToMySQL.cloud_server = properties.getProperty("cloud_server");
			CloudToMySQL.cloud_topic = properties.getProperty("cloud_topic");
			//CloudToMySQL.user = properties.getProperty("user");
			//CloudToMySQL.password = properties.getProperty("password");
			CloudToMySQL.delete_many = properties.getProperty("delete_many");
			CloudToMySQL.server_local = properties.getProperty("server_local");

		} catch (Exception obj) {
		
			System.out.println("Error reading CloudToMySQL.ini file " + obj);
			JOptionPane.showMessageDialog(null, "The CloudToMySQL.inifile wasn't found.", "CloudToMySQL", 0);
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel carregar o ficheiro CloudToMySQL.ini " + ts + "\n");
				appendLine.close();
				
				System.exit(0);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
		}
		new CloudToMySQL().connecCloud();

		try {
			
			//conexao a base de dados MySQL
			String connectionString = "jdbc:mysql://localhost:3306/" + CloudToMySQL.server_local +  "?useSSL=false";
			connect = DriverManager.getConnection(connectionString,
					CloudToMySQL.user, CloudToMySQL.password);
			CloudToMySQL.documentLabel.append("Ligacao a MySQL estabelecida!" + "\n");
			System.out.println("Ligacao a MySQL estabelecida!");
			//createWindow();
			//deleteAllEntriesInMySQL();

		} catch (SQLException ex) {

			System.out.println("Nao foi possivel ligar ao servidor MySQL. Verifique as ligacoes e credenciais!");
			CloudToMySQL.documentLabel.append("Nao foi possivel ligar ao servidor MySQL!" + "\n");
			CloudToMySQL.documentLabel.append("Verifique as ligacoes e reinicie o programa!" + "\n");
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel ligar ao servidor MySQL. Verifique as ligacoes e credenciais " + ts + "\n");
				appendLine.close();
				
				System.exit(0);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		createWindow();
		deleteAllEntriesInMySQL();
	}

	public static void main(final String[] array) {
		
		firstWindow();
	}
	
	public static void deleteAllEntriesInMySQL() {
		
		if(CloudToMySQL.delete_many.equals("true") && connect != null) {
			
			//eliminar todas as entradas anteriores na tabela
			//para efeito de alteracao de medicoes ou limites dos sensores
			try {
				
				Statement stm = connect.createStatement();
				String query = "DELETE FROM medicao";
				System.out.println("Executing Query: " + query);
				stm.executeUpdate(query);
				CloudToMySQL.documentLabel.append(query + " executed!" + "\n");
				CloudToMySQL.documentLabel.append("Entradas anteriores eliminadas, MySQL pronto a receber medicoes!" + "\n");
				System.out.println("Entradas anteriores eliminadas, MySQL pronto a receber novas medicoes!");
				Statement alter = connect.createStatement();
				String query2 = "ALTER TABLE 'medicao' AUTO_INCREMENT=1";
				System.out.println("Executing Query: " + query2);
				alter.executeUpdate(query);
				CloudToMySQL.documentLabel.append(query2 + " executed!" + "\n");
				CloudToMySQL.documentLabel.append("Auto-incremento alterado, MySQL pronto a receber medicoes!" + "\n");
				System.out.println("Auto-incremento alterado, MySQL pronto a receber novas medicoes!");
		
			} catch (SQLException ex) {
			
				CloudToMySQL.documentLabel.append("Nao foi possivel limpar a tabela medicao!" + "\n");
				System.out.println("Nao foi possivel limpar a tabela medicao!");
				Date date = new Date();
		        Timestamp ts=new Timestamp(date.getTime());
		       
		        try {
		        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
					appendLine.append("Nao foi possivel limpar a tabela medicao " + ts + "\n");
					appendLine.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		}

	public void connecCloud() {
		
		//conexao ao servidor MQTT
		try {
			(this.mqttclient = new MqttClient(CloudToMySQL.cloud_server,
					"CloudToMySQL" + String.valueOf(new Random().nextInt(100000)) + "_" + CloudToMySQL.cloud_topic))
							.connect();
			this.mqttclient.setCallback((MqttCallback) this);
			this.mqttclient.subscribe(CloudToMySQL.cloud_topic);
			CloudToMySQL.documentLabel.append("Ligacao ao servidor broker estabelecida!" + "\n");
			System.out.println("Ligacao ao servidor broker estabelecida!");
		} catch (MqttException ex) {
			ex.printStackTrace();
			System.out.println("Nao foi possivel ligar ao broker. Reinicie o programa!");
			CloudToMySQL.documentLabel.append("Nao foi possivel ligar ao broker. Reinicie o programa!" + "\n");
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

	public void messageArrived(final String s, final MqttMessage mqttMessage) throws Exception {
		
		//metodo que e executado quando chega uma mensagem do servidor MQTT
		
		try {
			
			processJson(mqttMessage.toString());
		} catch (Exception x) {
			if(!terminated) {
			System.out.println("E necessario iniciar primeiro o programa CloudToMySQL no PC2!");
			CloudToMySQL.documentLabel.append("E necessario iniciar primeiro o programa CloudToMySQL no PC2" + "\n");
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
			
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("E necessario iniciar primeiro o programa CloudToMySQL no PC2 " + ts + "\n");
				appendLine.close();
				try {
					final MqttMessage mqttMessage2 = new MqttMessage();
					String ss = "STOP.2";
					mqttMessage2.setPayload(ss.getBytes());
					CloudToMySQL.mqttclient.publish(CloudToMySQL.cloud_topic, mqttMessage2);
				} catch (MqttException e) {
					e.printStackTrace();
				}
				System.exit(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		}
		
	}

	public void connectionLost(final Throwable t) {

		//como reagir quando ha perda de ligacao ao MQTT
		System.out.println(t.fillInStackTrace());
		CloudToMySQL.documentLabel.append("Houve perda de conexao com o servidor broker!" + "\n");
		Date date = new Date();
        Timestamp ts=new Timestamp(date.getTime());
       
        try {
        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
			appendLine.append("Houve perda de ligacao com o servidor broker " + ts + "\n");
			appendLine.close();
			System.exit(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deliveryComplete(final IMqttDeliveryToken mqttDeliveryToken) {
		
		//como reagir quando e enviada uma mensagem de finalizacao da tranferencia
		//nunca acontece
		CloudToMySQL.documentLabel.append("A transferencia de mensagens foi concluida!" + "\n");
		System.out.println("A transferencia de mensagens foi concluida!");
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
			Statement stmt = connect.createStatement();
			System.out.println("Executing Query: " + query);
			stmt.executeUpdate(query);
			numberSent++;
			
			CloudToMySQL.documentLabel.append(query);
			CloudToMySQL.documentLabel.append("O objeto foi introduzido na base de dados MySQL!\n");
			CloudToMySQL.documentLabel.append(numberSent + " objetos enviados!\n");

		} catch (SQLException ex) {

			System.out.println("Nao foi possivel introduzir o objeto na base de dados MySQL devido a perda de ligacao com a mesma!");
			CloudToMySQL.documentLabel.append(json + "\n");
			CloudToMySQL.documentLabel.append("Nao foi possivel introduzir o objeto na base de dados MySQL devido a perda de ligacao com a mesma!\n");
			Date date = new Date();
	        Timestamp ts=new Timestamp(date.getTime());
	       
	        try {
	        	BufferedWriter appendLine = new BufferedWriter(new FileWriter(myObj, true));
				appendLine.append("Nao foi possivel introduzir o objeto na base de dados MySQL devido a perda de ligacao com a mesma " + ts + "\n");
				appendLine.close();
				try {
					final MqttMessage mqttMessage = new MqttMessage();
					String s = "STOP";
					mqttMessage.setPayload(s.getBytes());
					CloudToMySQL.mqttclient.publish(CloudToMySQL.cloud_topic, mqttMessage);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (MqttException e) {
					e.printStackTrace();
				}
				System.exit(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	static {
		CloudToMySQL.cloud_server = new String();
		CloudToMySQL.cloud_topic = new String();
		//CloudToMySQL.user = new String();
		//CloudToMySQL.password = new String();
		CloudToMySQL.documentLabel = new JTextArea("\n");
		CloudToMySQL.delete_many = new String();
		CloudToMySQL.server_local = new String();
	}
}