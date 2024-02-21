package com.cottagecoders;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.count;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;

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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoDB {

  private static final String DB_NAME = "bobdb";
  private static final String PEOPLE_COLLECTION = "people";

  public static void main(String[] args) {

    MongoDB mdb = new MongoDB();
    mdb.run();
  }

  void run() {
    String connectionString =
        String.format(
            "mongodb+srv://%s:%s@%s.mongodb.net/%s?retryWrites=true&w=majority",
            System.getenv("MONGODB_USERNAME"),
            System.getenv("MONGODB_PASSWORD"),
            System.getenv("MONGODB_CLUSTER_ID"),
            System.getenv(DB_NAME));

    ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();
    MongoClientSettings settings =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(connectionString))
            .serverApi(serverApi)
            .build();

    long start = 0;
    long numdocs = 0;
    try (MongoClient client = MongoClients.create(settings)) {
      ping(client);

      MongoDatabase mdb = client.getDatabase(DB_NAME);
      MongoCollection coll = mdb.getCollection(PEOPLE_COLLECTION);
      start = System.currentTimeMillis();
      numdocs = insertSomeDocuments(coll, DB_NAME, PEOPLE_COLLECTION, 1_000_000);

      insertOneDocument(client, DB_NAME, PEOPLE_COLLECTION, new Document().append("name", "matt"));
      //      deleteAllDocuments(client, DB_NAME, PEOPLE_COLLECTION);

      // drop output collection.
      mdb.getCollection("res").drop();
      aggregateOne(mdb);

      coll = mdb.getCollection(PEOPLE_COLLECTION);
      System.out.println("document count " + coll.countDocuments());

      //    deleteOurDatabase(client, DB_NAME);

      listDatabases(client);

      printSomeZips(client);
      printSomePeople(client, DB_NAME, PEOPLE_COLLECTION, 100);

      insertOneDocument(
          client,
          DB_NAME,
          PEOPLE_COLLECTION,
          new Document().append("name", "bob").append("town", "da bucket"));

    } catch (Exception ex) {
      System.out.println("Exception: " + ex.getMessage());
      ex.printStackTrace();

    } finally {
      long elapsed = System.currentTimeMillis() - start;
      System.out.println("elapsed " + elapsed + " numdocs: " + numdocs);
    }
  }

  void aggregateOne(MongoDatabase mdb) {

    Document regex = new Document();
    regex.append("$regex", "^(?)" + Pattern.quote("m"));
    regex.append("$options", "i");

    Bson match = match(eq("name", regex));
    Bson sort = sort(ascending("_id"));
    Bson group = group("$catname", sum("count", 1));
    //    Bson project = project(fields(include("catname")));

    MongoCollection<Document> coll = mdb.getCollection(PEOPLE_COLLECTION);

    List<Document> res = coll.aggregate(List.of(match, group, sort)).into(new ArrayList());


    res.forEach(
        d -> {
          System.out.println(d.toString());
        });

    mdb.getCollection("res").insertMany(res);

    System.out.println("count from res " + coll.countDocuments());
  }

  void deleteOurDatabase(MongoClient client, String dbName) {
    client.getDatabase(dbName).drop();
  }

  void deleteAllDocuments(MongoClient client, String dbName, String collection) {
    client.getDatabase(dbName).getCollection(collection).drop();
  }

  void insertOneDocument(MongoClient client, String dbName, String collection, Document doc) {
    InsertOneResult res = client.getDatabase(dbName).getCollection(collection).insertOne(doc);
    System.out.println("insertOneDocument acknowledged? " + res.wasAcknowledged());
    System.out.println("InsertOneDocument insertedId: " + res.getInsertedId());
    System.out.println(
        "InertOnedocument inserted objectId: " + res.getInsertedId().asObjectId().getValue());
  }

  void ping(MongoClient client) {
    // Send a ping to confirm a successful connection
    MongoDatabase database = client.getDatabase("admin");
    Document d = database.runCommand(new Document("ping", 1));
    System.out.println("Pinged your deployment. You successfully connected to MongoDB! " + d);
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

  long insertSomeDocuments(MongoCollection coll, String dbName, String collection, int numDocs) {
    Faker faker = new Faker();

    List<Document> docs = new ArrayList<>();
    int i = 1;
    for (; i <= numDocs; i++) {
      try {
        docs.add(
            new Document()
                .append("name", faker.name().name())
                .append(
                    "address",
                    new Document()
                        .append("town", faker.address().city())
                        .append("state", faker.address().state())
                        .append("zip", faker.address().zipCode()))
                .append("phone", faker.phoneNumber().phoneNumber())
                .append("email", faker.internet().emailAddress())
                .append("catname", i % 23 == 0 ? "Mickey" : faker.cat().name()));

        if (i % 25000 == 0) {
          coll.insertMany(docs);
          docs.clear();
        }
      } catch (Exception ex) {
        return i;
      }
    }
    return i;
  }

  void printSomePeople(MongoClient client, String dbName, String collection, int count) {
    FindIterable<Document> ordered =
        client
            .getDatabase(dbName)
            .getCollection(collection)
            .find()
            .limit(count)
            .sort(ascending("name"));
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
    FindIterable<Document> zips =
        client.getDatabase("sample_training").getCollection("zips").find(eq("city", "BROOKLYN"));
    zips.forEach(z -> System.out.println("printAllZips: " + z.toString()));
  }
}
