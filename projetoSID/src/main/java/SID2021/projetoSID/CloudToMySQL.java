package SID2021.projetoSID;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import twitter4j.JSONObject;

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
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import java.sql.*;

public class CloudToMySQL implements MqttCallback {

	MqttClient mqttclient;
	static String cloud_server;
	static String cloud_topic;
	static String user;
	static String password;
	static JTextArea documentLabel;
	static Connection connect = null;
	static int numberSent;
	int id = 0;

	private static void createWindow() {

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
		comp2.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(final ActionEvent actionEvent) {
				System.exit(0);
			}
		});
	}

	public static void main(final String[] array) {

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");

		} catch (Exception e) {
			System.out.println("Nao foi possivel carregar o driver");
		}

		try {
			final Properties properties = new Properties();
			properties.load(CloudToMySQL.class.getResourceAsStream("/CloudToMySQL.ini"));
			CloudToMySQL.cloud_server = properties.getProperty("cloud_server");
			CloudToMySQL.cloud_topic = properties.getProperty("cloud_topic");
			CloudToMySQL.user = properties.getProperty("user");
			CloudToMySQL.password = properties.getProperty("password");

		} catch (Exception obj) {
			System.out.println("Error reading CloudToMySQL.ini file " + obj);
			JOptionPane.showMessageDialog(null, "The CloudToMySQL.inifile wasn't found.", "Receive Cloud", 0);
		}
		new CloudToMySQL().connecCloud();

		try {
			connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/culturas?useSSL=false",
					CloudToMySQL.user, CloudToMySQL.password);
			CloudToMySQL.documentLabel.append("Ligacao a MySQL estabelecida!" + "\n");
			System.out.println("Ligacao a MySQL estabelecida!");

		} catch (SQLException ex) {

			System.out.println("Nao foi possivel ligar ao servidor MySQL!");
			System.out.println("Verifique as ligacoes e reinicie o programa!");
			CloudToMySQL.documentLabel.append("Nao foi possivel ligar ao servidor MySQL!" + "\n");
			CloudToMySQL.documentLabel.append("Verifique as ligacoes e reinicie o programa!" + "\n");
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		}
		
		createWindow();
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
		} catch (SQLException ex) {
			
			CloudToMySQL.documentLabel.append("Nao foi possivel limpar a tabela medicao!" + "\n");
			System.out.println("Nao foi possivel limpar a tabela medicao!");
		}
	}

	public void connecCloud() {
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
		}
	}

	public void messageArrived(final String s, final MqttMessage mqttMessage) throws Exception {
		try {
			id++;
			processJson(mqttMessage.toString(), id);
		} catch (Exception x) {
			System.out.println("Nao foi possivel carregar o objeto para a base de dados MySQL!");
			CloudToMySQL.documentLabel.append("Nao foi possivel carregar o objeto para a base de dados MySQL" + "\n");
		}
	}

	public void connectionLost(final Throwable t) {

		System.out.println(t.fillInStackTrace());
		CloudToMySQL.documentLabel.append("Houve perda de conexao com o servidor broker!" + "\n");
	}

	public void deliveryComplete(final IMqttDeliveryToken mqttDeliveryToken) {
		
		CloudToMySQL.documentLabel.append("A transferencia de mensagens foi concluida!" + "\n");
		System.out.println("A transferencia de mensagens foi concluida!");
	}

	private void processJson(String json, int id) {

		JSONObject jsonObject = new JSONObject(json);
		JSONObject register = jsonObject.getJSONObject("doc");
		//int i = Integer.parseInt(jsonObject.get("id").toString());
		String zona = register.getString("Zona");
		String sensor = register.getString("Sensor");
		
		String timestamp = register.getString("Data");
		double measure = register.getDouble("Medicao");
		boolean valid = false;
		String IDZona;
		if (zona.contentEquals("Z1")) {
			IDZona = "1";
		} else {
			IDZona = "2";
		}
		String Tipo = String.valueOf(sensor.charAt(0));
		String IDSensor = String.valueOf(sensor.charAt(1));
		int IDSensor2 = Character.getNumericValue(sensor.charAt(1));
		try {

			int sensorID = 0;
			System.out.println(sensor);
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
			System.out.println(sensorID);
			Statement limitSuperior = connect.createStatement();
			Statement limitInferior = connect.createStatement();
			ResultSet rs = limitSuperior.executeQuery("SELECT LimiteSuperior FROM sensor WHERE IDSensor =" + sensorID);
			ResultSet ra = limitInferior.executeQuery("SELECT LimiteInferior FROM sensor WHERE IDSensor =" + sensorID);
			System.out.println(rs);
			int limitSup = 0;
			int limitInf = 0;
			while (rs.next()) {
				limitSup = rs.getInt(1);

			}
			while (ra.next()) {
				limitInf = ra.getInt(1);

			}
			if (limitSup < measure || measure < limitInf) {
				valid = false;
			} else {
				valid = true;
			}
			int Zona2 = Integer.parseInt(IDZona);
			String query = "INSERT INTO medicao (IDMedicao, Zona, Tipo, IDSensor, Hora, Leitura, valid) " + "VALUES (" + id
					+ ", '" + Zona2 + "', '" + Tipo + "', " + sensorID + ", '" + timestamp + "', '" + measure + "', " + valid + ")";
			Statement stmt = connect.createStatement();
			System.out.println("Executing Query: " + query);
			stmt.executeUpdate(query);
			numberSent++;
			CloudToMySQL.documentLabel.append("{id: " + id + ", doc:{'Zona': '" + Zona2 + "', 'Sensor': '" + sensor + "', 'Data': '" + timestamp + "', 'Medicao': '" + measure + "'}}" + "\n");
			CloudToMySQL.documentLabel.append("O objeto foi introduzido na base de dados MySQL!\n");
			CloudToMySQL.documentLabel.append(numberSent + " objetos enviados!\n");

		} catch (SQLException ex) {

			System.out.println("Nao foi possivel introduzir o objeto na base de dados MySQL!");
			CloudToMySQL.documentLabel.append(json + "\n");
			CloudToMySQL.documentLabel.append("Nao foi possivel introduzir o objeto na base de dados MySQL!\n");
		}

	}

	static {
		CloudToMySQL.cloud_server = new String();
		CloudToMySQL.cloud_topic = new String();
		CloudToMySQL.user = new String();
		CloudToMySQL.password = new String();
		CloudToMySQL.documentLabel = new JTextArea("\n");
	}
}