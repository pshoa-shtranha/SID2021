package SID2021.projetoSID;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;

//import twitter4j.*;
import com.mongodb.client.*;


public class App {

    private static final int MAX_REGISTERS = 5000;

    public static String connectionString = "mongodb://aluno:aluno@194.210.86.10:27017/?authSource=admin";
    public static MongoClient mongoRaiz = MongoClients.create(connectionString);

    public static List<MongoCollection<Document>> q = new ArrayList<>();
    public static void main(String[] args) {

        MongoDatabase b = bd();

        MongoIterable<String> colecoes = b.listCollectionNames();
        MongoCursor<String> cursor2 = colecoes.cursor();
        while(cursor2.hasNext()) {
            q.add(b.getCollection(cursor2.next()));
        }

        MongoCollection<Document> collection = q.get(0);
        System.out.println("Collection " + collection.getNamespace() + " has " + collection.count() + " registers");

        promptUser();

        int count = 0;
        MongoCursor<Document> docCursor = collection.find().iterator();
        while (docCursor.hasNext() && count < MAX_REGISTERS) {
            Document doc = docCursor.next();
            System.out.println(doc.toJson());

            count++;
        }

    }

    private static MongoDatabase bd() {

        MongoDatabase a = null;

        MongoIterable<String> strings = mongoRaiz.listDatabaseNames();
        MongoCursor<String> cursor = strings.cursor();
        while(cursor.hasNext()) {
            String nome = cursor.next();
            if (nome.startsWith("sid2021")) {
                String b = "sid2021";
                a = mongoRaiz.getDatabase(b);
            }
        }
        if (a == null) {
            System.err.println("A base de dados com os sensores nao foi encontrada!");
        }

        return a;
    }

    private static void promptUser() {

        System.out.println("Do you wish to print the first " + MAX_REGISTERS + " registers (Y/N)?");
        Scanner kbScanner = new Scanner(System.in);

        boolean waitingInput = true;
        while (waitingInput) {
            // Blocks thread until user enters a input
            switch (kbScanner.next()) {
                case "Y":
                    waitingInput = false;
                    break;
                case "N":
                    // Kills program
                    System.exit(0);
                    break;
                default:
                    System.out.println("Please insert Y or N");
            }
        }
    }
}
