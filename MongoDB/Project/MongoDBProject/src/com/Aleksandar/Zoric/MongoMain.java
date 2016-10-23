/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.Aleksandar.Zoric;

/**
 *
 * @author Aleks
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.ParseException;
import java.util.Collection;

import java.util.Date;

public class MongoMain {

    //Creating a Mongo client and creating/retrieving the database
    MongoClient mongoClient = new MongoClient();
    MongoDatabase db = mongoClient.getDatabase("amarokforumdb");

    public static void main(String args[]) {

    }

    
    //This method enables the user to insert a new gamer/user to the forum
    public void insertUser(String name, String email, Date dob, Date dateJoined, String gamerTag, String game) throws ParseException {
        db.getCollection("users");
        db.getCollection("users").insertOne(
                new Document()
                .append("name", name)
                .append("email", email)
                .append("dob", dob)
                .append("dateJoined", dateJoined)
                .append("gamerTag", gamerTag)
                .append("game", game));
    }

    
    
    //This method enables the user to insert a new comment
    public void insertComment(String discussion, Date datePosted, String author, String comment) {
        db.getCollection("comments").insertOne(
                new Document()
                .append("Discussion", discussion)
                .append("datePosted", datePosted)
                .append("Author", author)
                .append("Comment", comment));
    }

    
    //This method enables the user to update an existing user/gamer
    public void updateUser(String name, String gamerTag) {
        db.getCollection("users").updateOne(new Document("name", name),
                new Document("$set", new Document("gamerTag", gamerTag)));
    }

    
    //This methid enables the user to delete a current gamer/user in the database
    public void deleteUser(String name) {
        db.getCollection("users").deleteMany(new Document("name", name));
    }

    
    //This method enables the user to delete an exiting comment
    public void deleteComment(String author) {
        db.getCollection("comments").deleteMany(new Document("Author", author));
    }

    
    //This is the map/reduce function which catergorizes a section of comments in either old or new comments.
    //It then sums the total amount of documents/comments in the given section and displays it to the user,
    //Along with which category they are located in
    public String mapReduceFunction() {
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        DB db = mongoClient.getDB("amarokforumdb");
        DBCollection collection = db.getCollection("comments");
        long count = db.getCollection("comments").count();

        System.out.println("Current amount of documents: " + count);

        String map = "function() { "
                + "var category; "
                + "var numOfDocuments = " + count + ";"
                + "for(i = 0; i < numOfDocuments; i++){ "
                + "if (numOfDocuments <= 100) {"
                + "category = 'New Comments'; }"
                + "else if(numOfDocuments > 100){"
                + "category = 'Old Comments'; }}"
                + "emit(category,1);};";

        String reduce = "function(key, values) { "
                + "var sum = 0; "
                + "values.forEach(function(doc) { "
                + "sum += 1; "
                + "}); "
                + "return {comments: sum};} ";

        MapReduceCommand cmd = new MapReduceCommand(collection, map,
                reduce, null, MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput out = collection.mapReduce(cmd);

        System.out.println("Mapreduce results");

        String result = null;

        for (DBObject o : out.results()) {
            result += o;

        }
        return result;
    }

    
    //This method count the amount of documents in a given collection
    public long numOfDocuments(String collection) {
        long numOfDocuments = db.getCollection(collection).count();

        return numOfDocuments;

    }

    
    //This method retrieves all the user's names in the current database
    public String retreiveUser() {
        DB db = mongoClient.getDB("amarokforumdb");
        DBCollection collection = db.getCollection("users");

        BasicDBObject allQuery = new BasicDBObject();
        BasicDBObject fields = new BasicDBObject();
        fields.put("name", 1);
        String result = null;
        DBCursor cursor = collection.find(allQuery, fields);
        while (cursor.hasNext()) {
            result += "\n" + cursor.next();
            //System.out.println(cursor.next());
        }
        return "All current user's are as follows: \n" + result;
    }

}
