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
import java.util.regex.PatternSyntaxException;

import javax.swing.JOptionPane;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import java.awt.Component;
import javax.swing.JScrollPane;
import java.awt.Dimension;
import javax.swing.JLabel;
import javax.swing.JFrame;
import javax.swing.JTextArea;
import com.mongodb.DBCollection;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;

// 
// Decompiled by Procyon v0.5.36
// 

public class MongoToCloud implements MqttCallback
{
    static MqttClient mqttclient;
    static DBCollection table;
    static String mongo_user;
    static String mongo_password;
    static String cloud_server;
    static String cloud_topic;
    static String mongo_replica;
    static String mongo_address;
    static String mongo_database;
    static String mongo_collection;
    static String mongo_criteria;
    static String mongo_fieldquery;
    static String mongo_fieldvalue;
    static String delete_document;
    static String loop_query;
    static String create_backup;
    static String backup_collection;
    static String display_documents;
    static String seconds_wait;
    static JTextArea documentLabel;
    static String mongo_authentication;
    
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
        String[] colecoes = null;
        try {
        	
            final Properties properties = new Properties();
            properties.load(new FileInputStream("C:\\Users\\Visha\\git\\SID2021\\projetoSID\\src\\main\\java\\SID2021\\projetoSID\\MongoToCloud.ini"));
            //String q = properties.getProperty("mongo_database");
            //try {
            //colecoes = q.split(".");
            //} catch (PatternSyntaxException e) {
            	
            	//System.out.println("Tem de utilizar o caracter correto na designação das colecoes!");
            //}
            //Thread[] varias = new Thread[colecoes.length];
			//for(int i = 0; i < colecoes.length; i++) {
            	//varias[i] = 
            //}
			
            MongoToCloud.mongo_address = properties.getProperty("mongo_address");
            MongoToCloud.mongo_user = properties.getProperty("mongo_user");
            MongoToCloud.mongo_password = properties.getProperty("mongo_password");
            //MongoToCloud.mongo_database = properties.getProperty("mongo_database");
            MongoToCloud.mongo_collection = properties.getProperty("mongo_collection");
            MongoToCloud.mongo_fieldquery = properties.getProperty("mongo_fieldquery");
            MongoToCloud.mongo_fieldvalue = properties.getProperty("mongo_fieldvalue");
            MongoToCloud.delete_document = properties.getProperty("delete_document");
            MongoToCloud.create_backup = properties.getProperty("create_backup");
            MongoToCloud.backup_collection = properties.getProperty("backup_collection");
            MongoToCloud.display_documents = properties.getProperty("display_documents");
            MongoToCloud.mongo_authentication = properties.getProperty("mongo_authentication");
            MongoToCloud.mongo_replica = properties.getProperty("mongo_replica");
            MongoToCloud.loop_query = properties.getProperty("loop_query");
            MongoToCloud.seconds_wait = properties.getProperty("delay");
            MongoToCloud.cloud_server = properties.getProperty("cloud_server");
            MongoToCloud.cloud_topic = properties.getProperty("cloud_topic");
            
            String q = properties.getProperty("mongo_database");
            try {
            	colecoes = q.split(".");
            } catch (PatternSyntaxException e) {
        	
            	System.out.println("Tem de utilizar o caracter correto na designação das colecoes!");
            }
            Thread[] varias = new Thread[colecoes.length];
            MongoToCloud contador = new MongoToCloud();
            for(int i = 0; i < varias.length; i++) {
            	String atual = colecoes[i];
            	varias[i] = contador.new incrementador(atual);
            	
            }
            for(Thread t:varias) {
            	
            	t.start();
            }
        }
        catch (Exception obj) {
            System.out.println("Error reading MongoToCloud.ini file " + obj);
            JOptionPane.showMessageDialog(null, "The MongoToCloud inifile wasn't found.", "Mongo To Cloud2", 0);
        }
        //new MongoToCloud().connecCloud();
        //new MongoToCloud().jsonToCloud();
    }
    
    public class incrementador extends Thread {
    	
    	public String colecao;
    	incrementador(String colecao) {
    		
    		this.colecao = colecao;
    	}
    	public void run() {
    		MongoToCloud.mongo_database = colecao;
    		new MongoToCloud().connecCloud();
            new MongoToCloud().jsonToCloud(colecao);
    	}
    }
    
    protected String getSaltString() {
        final String s = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        final StringBuilder sb = new StringBuilder();
        final Random random = new Random();
        while (sb.length() < 18) {
            sb.append(s.charAt((int)(random.nextFloat() * s.length())));
        }
        return sb.toString();
    }
    
    public void connecCloud() {
        try {
            (MongoToCloud.mqttclient = new MqttClient(MongoToCloud.cloud_server, "MongoToCloud" + this.getSaltString() + MongoToCloud.cloud_topic)).connect();
            MongoToCloud.mqttclient.setCallback((MqttCallback)this);
            MongoToCloud.mqttclient.subscribe(MongoToCloud.cloud_topic);
            MongoToCloud.documentLabel.append("Connection To Cloud Suceeded\n");
        }
        catch (MqttException ex) {
            ex.printStackTrace();
        }
    }
    
    public void jsonToCloud(String colecao) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        final String s = new String();
        final String s2 = new String();
        String string = "mongodb://";
        if (MongoToCloud.mongo_authentication.equals("true")) {
            string = string + MongoToCloud.mongo_user + ":" + MongoToCloud.mongo_password + "@";
        }
        String str = string + MongoToCloud.mongo_address;
        if (!MongoToCloud.mongo_replica.equals("false")) {
            if (MongoToCloud.mongo_authentication.equals("true")) {
                str = str + "/?replicaSet=" + MongoToCloud.mongo_replica + "&authSource=admin";
            }
            else {
                str = str + "/?replicaSet=" + MongoToCloud.mongo_replica;
            }
        }
        else if (MongoToCloud.mongo_authentication.equals("true")) {
            str += "/?authSource=admin";
        }
        final MongoDatabase database = new MongoClient(new MongoClientURI(str)).getDatabase(MongoToCloud.mongo_database);
        MongoToCloud.documentLabel.append("Connection To Mongo Suceeded\n");
        final MongoCollection collection = database.getCollection(MongoToCloud.mongo_collection);
        final MongoCollection collection2 = database.getCollection(MongoToCloud.backup_collection);
        final Document document = new Document();
        if (!MongoToCloud.mongo_fieldquery.equals("null")) {
            document.put(MongoToCloud.mongo_fieldquery, (Object)MongoToCloud.mongo_fieldvalue);
        }
        int i = 0;
        int j = 0;
        int k = 0;
        while (i == 0) {
            MongoToCloud.documentLabel.append("loop number ....." + j + "\n");
            final Date date = new Date(System.currentTimeMillis());
            MongoToCloud.documentLabel.append(simpleDateFormat.format(date) + "\n");
            System.out.println("loop number ....." + j + "\n");
            System.out.println(simpleDateFormat.format(date));
            this.writeSensor("{Loop:" + j + "}");
            final FindIterable find = collection.find((Bson)document);
            find.iterator();
            int n = 1;
            final MongoCursor iterator = find.projection(Projections.excludeId()).iterator();
            while (iterator.hasNext()) {
                ++j;
                ++n;
                ++k;
                final Document document2 = new Document();
                final Document document3 = (Document)iterator.next();
                final String string2 = "{id:" + k + ", doc:" + document3.toJson() + "}";
                if (MongoToCloud.display_documents.equals("true")) {
                	System.out.println(colecao);
                    MongoToCloud.documentLabel.append(string2 + "\n");
                }
                if (MongoToCloud.create_backup.equals("true")) {
                    collection2.insertOne((Object)document3);
                }
                this.writeSensor(string2);
                if (!MongoToCloud.seconds_wait.equals("0")) {
                    try {
                        Thread.sleep(Integer.parseInt(MongoToCloud.seconds_wait));
                    }
                    catch (Exception ex) {}
                }
            }
            if (MongoToCloud.delete_document.equals("true")) {
                if (!MongoToCloud.mongo_fieldquery.equals("null")) {
                    collection.deleteMany(Filters.eq(MongoToCloud.mongo_fieldquery, (Object)MongoToCloud.mongo_fieldvalue));
                }
                if (MongoToCloud.mongo_fieldquery.equals("null")) {
                    database.getCollection(MongoToCloud.mongo_collection).drop();
                }
            }
            if (!MongoToCloud.loop_query.equals("true")) {
                i = 1;
            }
        }
    }
    
    public void writeSensor(final String s) {
        try {
            final MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setPayload(s.getBytes());
            MongoToCloud.mqttclient.publish(MongoToCloud.cloud_topic, mqttMessage);
        }
        catch (MqttException ex) {
            ex.printStackTrace();
        }
    }
    
    public void connectionLost(final Throwable t) {
    }
    
    public void deliveryComplete(final IMqttDeliveryToken mqttDeliveryToken) {
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
        MongoToCloud.mongo_collection = new String();
        MongoToCloud.mongo_criteria = new String();
        MongoToCloud.mongo_fieldquery = new String();
        MongoToCloud.mongo_fieldvalue = new String();
        MongoToCloud.delete_document = new String();
        MongoToCloud.loop_query = new String();
        MongoToCloud.create_backup = new String();
        MongoToCloud.backup_collection = new String();
        MongoToCloud.display_documents = new String();
        MongoToCloud.seconds_wait = new String();
        MongoToCloud.documentLabel = new JTextArea("\n");
        MongoToCloud.mongo_authentication = new String();
    }
}