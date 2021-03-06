package SID2021.projetoSID;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import com.mongodb.util.JSON;
import com.mongodb.DBObject;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import com.mongodb.MongoClientURI;
import org.eclipse.paho.client.mqttv3.MqttException;
import java.util.Random;
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
import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;

// 
// Decompiled by Procyon v0.5.36
// 

public class CloudToMongo implements MqttCallback
{
    MqttClient mqttclient;
    static MongoClient mongoClient;
    static DB db;
    static DBCollection mongocol;
    static String mongo_user;
    static String mongo_password;
    static String mongo_address;
    static String cloud_server;
    static String cloud_topic;
    static String mongo_host;
    static String mongo_replica;
    static String mongo_database;
    static String mongo_collection;
    static String display_documents;
    static String mongo_authentication;
    static JTextArea documentLabel;
    
    private static void createWindow() {
        final JFrame frame = new JFrame("Cloud to Mongo");
        frame.setDefaultCloseOperation(3);
        final JLabel comp = new JLabel("Data from broker: ", 0);
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
            properties.load(new FileInputStream("cloudToMongo.ini"));
            CloudToMongo.mongo_address = properties.getProperty("mongo_address");
            CloudToMongo.mongo_user = properties.getProperty("mongo_user");
            CloudToMongo.mongo_password = properties.getProperty("mongo_password");
            CloudToMongo.mongo_replica = properties.getProperty("mongo_replica");
            CloudToMongo.cloud_server = properties.getProperty("cloud_server");
            CloudToMongo.cloud_topic = properties.getProperty("cloud_topic");
            CloudToMongo.mongo_host = properties.getProperty("mongo_host");
            CloudToMongo.mongo_database = properties.getProperty("mongo_database");
            CloudToMongo.mongo_authentication = properties.getProperty("mongo_authentication");
            CloudToMongo.mongo_collection = properties.getProperty("mongo_collection");
            CloudToMongo.display_documents = properties.getProperty("display_documents");
        }
        catch (Exception obj) {
            System.out.println("Error reading CloudToMongo.ini file " + obj);
            JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo", 0);
        }
        new CloudToMongo().connecCloud();
        new CloudToMongo().connectMongo();
    }
    
    public void connecCloud() {
        try {
            (this.mqttclient = new MqttClient(CloudToMongo.cloud_server, "CloudToMongo_" + String.valueOf(new Random().nextInt(100000)) + "_" + CloudToMongo.cloud_topic)).connect();
            this.mqttclient.setCallback((MqttCallback)this);
            this.mqttclient.subscribe(CloudToMongo.cloud_topic);
        }
        catch (MqttException ex) {
            ex.printStackTrace();
        }
    }
    
    public void connectMongo() {
        final String s = new String();
        String string = "mongodb://";
        if (CloudToMongo.mongo_authentication.equals("true")) {
            string = string + CloudToMongo.mongo_user + ":" + CloudToMongo.mongo_password + "@";
        }
        String str = string + CloudToMongo.mongo_address;
        if (!CloudToMongo.mongo_replica.equals("false")) {
            if (CloudToMongo.mongo_authentication.equals("true")) {
                str = str + "/?replicaSet=" + CloudToMongo.mongo_replica + "&authSource=admin";
            }
            else {
                str = str + "/?replicaSet=" + CloudToMongo.mongo_replica;
            }
        }
        else if (CloudToMongo.mongo_authentication.equals("true")) {
            str += "/?authSource=admin";
        }
        CloudToMongo.db = new MongoClient(new MongoClientURI(str)).getDB(CloudToMongo.mongo_database);
        CloudToMongo.mongocol = CloudToMongo.db.getCollection(CloudToMongo.mongo_collection);
    }
    
    public void messageArrived(final String s, final MqttMessage mqttMessage) throws Exception {
        try {
            CloudToMongo.mongocol.insert(new DBObject[] { (DBObject)JSON.parse(mqttMessage.toString()) });
            if (CloudToMongo.display_documents.equals("true")) {
                CloudToMongo.documentLabel.append(mqttMessage.toString() + "\n");
            }
        }
        catch (Exception x) {
            System.out.println(x);
        }
    }
    
    public void connectionLost(final Throwable t) {
    }
    
    public void deliveryComplete(final IMqttDeliveryToken mqttDeliveryToken) {
    }
    
    static {
        CloudToMongo.mongo_user = new String();
        CloudToMongo.mongo_password = new String();
        CloudToMongo.mongo_address = new String();
        CloudToMongo.cloud_server = new String();
        CloudToMongo.cloud_topic = new String();
        CloudToMongo.mongo_host = new String();
        CloudToMongo.mongo_replica = new String();
        CloudToMongo.mongo_database = new String();
        CloudToMongo.mongo_collection = new String();
        CloudToMongo.display_documents = new String();
        CloudToMongo.mongo_authentication = new String();
        CloudToMongo.documentLabel = new JTextArea("\n");
    }
}