package com.punyabagus.mongo;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Bagus Prasojo on 3/25/2017.
 */
public abstract class MongoInstance<T> {

    public static final String OBJECT_ID = "_id";
    public static final String ID = "id";

    public static MongoClient client;
    public static MongoDatabase database;
    public MongoCollection<Document> collection;
    public Class<T> genericClass;

    public MongoInstance(String database, String collection, Class<T> genericClass) {
        if (this.client == null) {
            this.client = new MongoClient();
        }

        if (this.database == null) {
            this.database = client.getDatabase(database);
        }

        this.collection = this.database.getCollection(collection);
        this.genericClass = genericClass;
    }

    public T insert(T entity) {
        Document doc = getDocFromBuilder(entity);

        collection.insertOne(doc);

        return getBuilderFromDoc(formatObjectId(doc));
    }

    public T getById(String id) {
        return getBuilderFromDoc(formatObjectId(collection.find(new Document(OBJECT_ID, new ObjectId(id))).first()));
    }

    public T update(T entity) {
        Document doc = getDocFromBuilder(entity);

        String id = doc.get(ID).toString();

        doc.remove(ID);

        collection.updateOne(new Document(OBJECT_ID, new ObjectId(id)), new Document("$set", doc));

        return entity;
    }

    public void removeField(String id, String fieldName) {
        collection.updateOne(new Document(OBJECT_ID, new ObjectId(id)), new Document("$unset", new Document(fieldName, "")));
    }

    public List<T> find(Document query) {
        List<T> result = new ArrayList();
        for (Document doc : collection.find(query)) {
            result.add(getBuilderFromDoc(formatObjectId(doc)));
        }
        return result;
    }

    private Document formatObjectId(Document doc) {
        if (doc != null && doc.containsKey(OBJECT_ID)) {
            doc.append(ID, doc.get(OBJECT_ID).toString()).remove(OBJECT_ID);
        }

        return doc;
    }

    private Document getDocFromBuilder(T entity){
        String document = new String();

        try {
            document = JsonFormat.printer().print((Message) entity);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Document.parse(document);
    }

    private T getBuilderFromDoc(Document doc) {
        if (doc == null) {
            return null;
        }

        Builder builder = null;

        try {
            Method newBuilderMethod = this.genericClass.getMethod("newBuilder");
            builder = (Builder) newBuilderMethod.invoke(null);
            JsonFormat.parser().merge(doc.toJson(), builder);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return (T) builder.build();
    }
}
