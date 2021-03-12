package lalss;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
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
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;

// 
// Decompiled by Procyon v0.5.36
// 

public class ReceiveCloud implements MqttCallback
{
    MqttClient mqttclient;
    static String cloud_server;
    static String cloud_topic;
    static JTextArea documentLabel;
    
    private static void createWindow() {
        final JFrame frame = new JFrame("Receive Cloud");
        frame.setDefaultCloseOperation(3);
        final JLabel comp = new JLabel("Data from broker: ", 0);
        comp.setPreferredSize(new Dimension(600, 30));
        ReceiveCloud.documentLabel.setPreferredSize(new Dimension(600, 200));
        final JScrollPane scrollPane = new JScrollPane(ReceiveCloud.documentLabel, 22, 32);
        frame.add(scrollPane);
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
        createWindow();
        try {
            final Properties properties = new Properties();
            properties.load(new FileInputStream("ReceiveCloud.ini"));
            ReceiveCloud.cloud_server = properties.getProperty("cloud_server");
            ReceiveCloud.cloud_topic = properties.getProperty("cloud_topic");
        }
        catch (Exception obj) {
            System.out.println("Error reading ReceiveCloud.ini file " + obj);
            JOptionPane.showMessageDialog(null, "The ReceiveCloud.inifile wasn't found.", "Receive Cloud", 0);
        }
        new ReceiveCloud().connecCloud();
    }
    
    public void connecCloud() {
        try {
            (this.mqttclient = new MqttClient(ReceiveCloud.cloud_server, "ReceiveCloud" + String.valueOf(new Random().nextInt(100000)) + "_" + ReceiveCloud.cloud_topic)).connect();
            this.mqttclient.setCallback((MqttCallback)this);
            this.mqttclient.subscribe(ReceiveCloud.cloud_topic);
        }
        catch (MqttException ex) {
            ex.printStackTrace();
        }
    }
    
    public void messageArrived(final String s, final MqttMessage mqttMessage) throws Exception {
        try {
            ReceiveCloud.documentLabel.append(mqttMessage.toString() + "\n");
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
        ReceiveCloud.cloud_server = new String();
        ReceiveCloud.cloud_topic = new String();
        ReceiveCloud.documentLabel = new JTextArea("\n");
    }
}