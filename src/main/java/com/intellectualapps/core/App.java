package com.intellectualapps.core;


import java.net.UnknownHostException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import java.sql.*;
import java.util.ArrayList;
 
public class App 
{
    
    
    //Mongodb connection variables
    protected String mongoUser = "";
    protected String mongoPassword = "";
    protected String mongoDb = "mongo_sample";
    protected int mongoPort = 27017;
    protected String mongoHost = "localhost";
    
    //mysql connection variables;
    protected  String mysqlUser = "root";
    protected  String mysqlPassword = "";
    protected  String mysqlDb = "mysql_sample";
    protected  String mysqlHost = "jdbc:mysql://localhost:3306/";
    
    
    /*
    *MongoDb connection method
    */
    public DB connectMongo(){
        DB connStatus = null;
        
        try{  
          MongoClient connectMongo = new MongoClient(mongoHost,mongoPort);
         
	  /**** Get database ****/
	  // if database doesn't exists, MongoDB will create it for you
	  DB mongo_db = connectMongo.getDB("mongo_sample");
          
          return mongo_db;
          
        }catch(UnknownHostException e){
          e.printStackTrace();
          
        }
        
        catch(MongoException e){
           e.printStackTrace();
          
        }
     
        
        return connStatus;
    }
    
   /*
    *Mysql connection method
    */
    public Connection connectMysql(){
        
        Connection connStatus = null;
        
        
        try{
            Class.forName("com.mysql.jdbc.Driver");  
            Connection mysqlConnect = DriverManager.getConnection(mysqlHost+mysqlDb,mysqlUser,mysqlPassword);
            return mysqlConnect;
        }catch(Exception e){
            e.printStackTrace();
        }
       return connStatus;       
    }
    
    /*
    *Method to watermark syncronization.
    *i.e, we need to know the last record that was inserted. 
    *This is to avoid duplicate sync.
    */
    public int fetchLastMysqlId(){
        int id = 0;
        Connection connObject = connectMysql();
        
        try{
              Statement stmt = connObject.createStatement();
              ResultSet rs = stmt.executeQuery("SELECT id FROM users ORDER BY id DESC LIMIT 1");
              while(rs.next()){
                 return rs.getInt("id");
              }
              
              return id;
         }catch(Exception e){
             e.printStackTrace();
         }finally
            {
              
                try {
                    //close connection
                     connObject.close();
                     
                   } catch(SQLException e) 
                     {
                       e.printStackTrace();
                    }
             }
   
        return id;
    }
    
    /**
     * Method to do synchronization
     */
    public void syncDb(){
        DB mongoObj = connectMongo();
        
        /**** Get collection / table from 'testdb' ****/
	// if collection doesn't exists, MongoDB will create it for you
	DBCollection table = mongoObj.getCollection("users"); 
       
        //Check if there is record in mysqlDb
        if(fetchLastMysqlId() > 0){
           
          DBCursor new_data = table.find(new BasicDBObject().append("id", new BasicDBObject().append("$gt",fetchLastMysqlId()) ));
          //Get number of new record
          int count_data = new_data.count();
         //check if new data exists in MongoDb 
         if(count_data > 0){

          Connection connObject = connectMysql();
         
          String [] queries = new String[count_data];
             
              //create counter for array index
             int counter=0;
             while(new_data.hasNext()){
               final DBObject value = new_data.next();
               queries[counter] = "INSERT INTO users (id,name,role) VALUES('"+
                       value.get("id") + "','"+
                       value.get("name")+ "','"+
                       value.get("role") +"')";
               counter++;
             }
            //System.out.println(queries[0]);
            try{
              Statement stmt = connObject.createStatement();
          
            for(String query : queries){
              stmt.executeUpdate(query);
            }
            System.out.println(count_data+" records Synched Successfully.");
            }catch(Exception e){
             e.printStackTrace();
            }finally
            {
              
                try {
                    //close connection
                     connObject.close();
                   } catch(SQLException e) 
                     {
                       e.printStackTrace();
                    }
             }
             
         }else{
            System.out.println("Nothing to sync."); 
         }
       
        }
        else
        {
          //This block only executes when mysql database is empty
          DBCursor users = table.find();
          Connection connObject = connectMysql();
          int total_rows = table.find().count();
          String [] queries = new String[total_rows];
          
          //Check if new records exist in mongoDb
          if(total_rows > 0){
              //create counter for array index
             int counter=0;
             while(users.hasNext()){
               final DBObject value = users.next();
               //Add insert statements into array for bulk insert
               queries[counter] = "INSERT INTO users (id,name,role) VALUES('"+
                       value.get("id") + "','"+
                       value.get("name")+ "','"+
                       value.get("role") +"')";
               counter++;
             }
          
            try{
              Statement stmt = connObject.createStatement();
          
            for(String query : queries){
              //Bulk insert new records
              stmt.executeUpdate(query);
            }
            System.out.println(total_rows+" records Synched Successfully.");
            }catch(Exception e){
             e.printStackTrace();
            }finally
            {
              
                try {
                    //close connection
                     connObject.close();
                   } catch(SQLException e) 
                     {
                       e.printStackTrace();
                    }
             }
          }else{
              System.out.println("Nothing to sync.");
          }
        }
        
    }

    public static void main( String[] args )
    {
        App mainObj = new App();
        System.out.println("Synching data......");
        //Call syncDb method to start syncronization
        mainObj.syncDb();
    
    }
}
