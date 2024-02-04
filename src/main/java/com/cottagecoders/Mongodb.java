package com.cottagecoders;

import com.github.javafaker.Faker;
import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;

public class Mongodb {
    public static void main(String[] args) {

        Mongodb mdb = new Mongodb();
        mdb.run();

    }

    void run() {
        String connectionString = String.format("mongodb+srv://%s:%s@%s.mongodb.net/?retryWrites=true&w=majority", System.getenv("MONGODB_USERNAME"), System.getenv("MONGODB_PASSWORD"), System.getenv("MONGODB_CLUSTER_ID"));

        ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString)).serverApi(serverApi).build();

        // Create a new client and connect to the server
        MongoClient mongoClient = MongoClients.create(settings);
        // Send a ping to confirm a successful connection
        MongoDatabase database = mongoClient.getDatabase("admin");
        database.runCommand(new Document("ping", 1));

        System.out.println("Pinged your deployment. You successfully connected to MongoDB!");

        // specify database...
        database = mongoClient.getDatabase("sample_training");

        // sqlect matching documents....
        FindIterable<Document> zips = database.getCollection("zips").find(eq("city", "BROOKLYN"));
        zips.forEach(z -> System.out.println(" zip " + z.toString()));

        // specify database
        database = mongoClient.getDatabase("bobdb");
        // add a row.
        database.getCollection("people").insertOne(new Document().append("name", "bob").append("town", "da bucket"));

        Faker faker = new Faker();
        for (int i = 0; i < 10000; i++) {
         //   database.getCollection("people").insertOne(new Document().append("name", faker.funnyName().name()).append("town", faker.address().city()));
        }

        FindIterable<Document> ordered = database.getCollection("people").find().limit(100).sort(ascending("name"));
        ordered.forEach(d -> System.out.println( "ordered " + d.toString()));

        try {
            ListDatabasesIterable<Document> dbs = mongoClient.listDatabases();
            dbs.forEach(db -> System.out.println("db " + db.toString()));

            for (Document doc : dbs) {
                System.out.println("---------");
                MongoDatabase db = mongoClient.getDatabase(doc.get("name").toString());
                ListCollectionsIterable<Document> colls = db.listCollections();
                colls.forEach(c -> System.out.println("coll " + c.toString()));
            }

        } catch (MongoException e) {
            e.printStackTrace();
        }

    }
}
