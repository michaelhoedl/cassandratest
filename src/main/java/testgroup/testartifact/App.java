package testgroup.testartifact;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Stopwatch;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
        System.out.println( "Hello World!!!" );
        
        String serverIP = "127.0.0.1";
        String keyspace = "example";
        
        Cluster cluster = Cluster.builder()
        		.addContactPoint(serverIP)
        		.build();
        Session session = cluster.connect(keyspace);
        
        

        
        String cqlStatement = "SELECT * FROM testdata limit 10;";
        
        // read all entries from users table
        for(Row row : session.execute(cqlStatement)) {
        	System.out.println(row.getString("txt"));
        	
        	Map<String,String> themap = row.getMap("something", String.class, String.class);
        	System.out.println("size: "+themap.size());
        	System.out.println("a="+themap.get("a"));
        	System.out.println("b="+themap.get("b"));
        }
        
        
        /*
        
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        PreparedStatement prep = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)");
        long starttime = System.nanoTime();
        
        // add new users
        for(int i = 1; i <= 100; i++) {
	        BoundStatement bound = new BoundStatement(prep);
	        bound.bind("'user"+i+"'", "'name"+i+"'");
	        batch.add(bound);
        }
        session.execute(batch);
        long endtime = System.nanoTime();
        System.out.println("Duration (ms): "+ (endtime-starttime)/1000000);
        */
        
        
        long starttime = System.nanoTime();
        PreparedStatement prep = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)");
        for(int i = 1; i <= 1000; i++) {
	        BoundStatement bound = new BoundStatement(prep);
	        bound.bind("'benutzer"+i+"'", "'name"+i+"'");
	        session.executeAsync(bound);
        }
        
        long endtime = System.nanoTime();
        System.out.println("Duration (ms): "+ (endtime-starttime)/1000000);
        
        // read all entries from users table
       // for(Row row : session.execute(cqlStatement)) {
       // 	System.out.println(row.toString());
       // }
        
        

        
        System.out.println("Done");
        session.close();
        cluster.close();
        System.exit(0);
        
    }
    
    
    


    
    
    
    
    
}
