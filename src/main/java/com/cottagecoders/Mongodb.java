package com.cottagecoders;

import com.github.javafaker.Faker;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;

public class Mongodb {

  private static final String DB_NAME = "bobdb";
  private static final String COLLECTION_NAME = "people";

  public static void main(String[] args) {

    Mongodb mdb = new Mongodb();
    mdb.run();

  }

  void run() {
    String connectionString = String.format("mongodb+srv://%s:%s@%s.mongodb.net/?retryWrites=true&w=majority", System.getenv("MONGODB_USERNAME"), System.getenv("MONGODB_PASSWORD"), System.getenv("MONGODB_CLUSTER_ID"));

    ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
    MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionString)).serverApi(serverApi).build();

    MongoClient client = MongoClients.create(settings);
    ping(client);

    deleteAllDocuments(client, DB_NAME, COLLECTION_NAME);

    listDatabases(client);

    //deleteOurDatabase(client, DB_NAME);

    printSomeZips(client);
    printSomePeople(client, DB_NAME, COLLECTION_NAME, 100);

//        insertOneDocument(client, DB_NAME, COLLECTION_NAME, new Document().append("name", "bob").append("town", "da bucket"));
    insertSomeDocuments(client, DB_NAME, COLLECTION_NAME, 10000);

  }

  void deleteOurDatabase(MongoClient client, String dbName) {
    client.getDatabase(dbName).drop();
  }

  void deleteAllDocuments(MongoClient client, String dbName, String collection) {
    client.getDatabase(dbName).getCollection(collection).deleteMany(Filters.exists("name"));

  }

  void insertOneDocument(MongoClient client, String dbName, String collection, Document doc) {
    client.getDatabase(dbName).getCollection(collection).insertOne(doc);
  }

  void ping(MongoClient client) {
    // Send a ping to confirm a successful connection
    MongoDatabase database = client.getDatabase("admin");
    Document d = database.runCommand(new Document("ping", 1));
    System.out.println("Pinged your deployment. You successfully connected to MongoDB!" + d);

  }

  void listDatabases(MongoClient client) {
    ListDatabasesIterable<Document> dbs = client.listDatabases();
    dbs.forEach(db -> System.out.println("db " + db.toString()));

  }

  void listDatabasesAndCollections(MongoClient client) {
    ListDatabasesIterable<Document> dbs = client.listDatabases();
    for (Document doc : dbs.authorizedDatabasesOnly(true)) {
      MongoDatabase db = client.getDatabase(doc.get("name").toString());
      ListCollectionsIterable<Document> colls = db.listCollections();

    }
  }

  void insertSomeDocuments(MongoClient client, String dbName, String collection, int numDocs) {
    Faker faker = new Faker();
    MongoDatabase db = client.getDatabase(dbName);

    for (int i = 0; i < numDocs; i++) {
      db.getCollection(collection).insertOne(new Document().append("name", faker.name().name()).append("town", faker.address().city()).append("phone", faker.phoneNumber().phoneNumber()).append("email", faker.internet().emailAddress()).append("catname", faker.cat().name()));

    }
  }

  void printSomePeople(MongoClient client, String dbName, String collection, int count) {
    FindIterable<Document> ordered = client.getDatabase(dbName).getCollection(collection).find().limit(count).sort(ascending("name"));
    ordered.forEach(d -> System.out.println("printSomePeople: " + d.toString()));

  }

  void printSomeZips(MongoClient client) {
    // specify database...
    MongoDatabase db = client.getDatabase("sample_training");
    // select matching documents....
    FindIterable<Document> zips = db.getCollection("zips").find(eq("city", "BROOKLYN"));
    zips.forEach(z -> System.out.println("printSomeZips: " + z.toString()));

  }

  void printAllZips(MongoClient client, int count) {
    // specify database...
    // sqlect matching documents....
    FindIterable<Document> zips = client.getDatabase("sample_training").getCollection("zips").find(eq("city", "BROOKLYN"));
    zips.forEach(z -> System.out.println("printAllZips: " + z.toString()));

  }

}
