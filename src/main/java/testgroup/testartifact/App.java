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

import utils.ConnectionHelperCassandra;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
        System.out.println( "Hello World!!!" );
        long starttime;
        long endtime;


        Session session = ConnectionHelperCassandra.getDBConnection();
        
        
        
        

        /*
        String cqlStatement = "SELECT * FROM testdata limit 10;";
        
        starttime = System.nanoTime();
        // read all entries from users table
        for(Row row : session.execute(cqlStatement)) {
        	System.out.println(row.toString());
        	
        	System.out.println(row.getString("txt"));
        	
        	Map<String,String> themap = row.getMap("something", String.class, String.class);
        	System.out.println("size: "+themap.size());
        	System.out.println("a="+themap.get("a"));
        	System.out.println("b="+themap.get("b"));
        }
        endtime = System.nanoTime();
        System.out.println("Duration SELECT (ms): "+ (endtime-starttime)/1000000);
        */
        
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
        
        
        /*
        starttime = System.nanoTime();
        PreparedStatement prep = session.prepare("INSERT INTO users (id, name) VALUES (?, ?)");
        for(int i = 1; i <= 1000; i++) {
	        BoundStatement bound = new BoundStatement(prep);
	        bound.bind("'benutzer"+i+"'", "'name"+i+"'");
	        session.executeAsync(bound);
        }
        
        endtime = System.nanoTime();
        System.out.println("Duration INSERT (ms): "+ (endtime-starttime)/1000000);
        */
        
        
        starttime = System.nanoTime();
        PreparedStatement prep = session.prepare("INSERT INTO test (testid, testname, testnum) VALUES (?, ?, ?)");
        for(int i = 1; i <= 1011; i++) {
	        BoundStatement bound = new BoundStatement(prep);
	        bound.bind("test"+i+"", "name"+i+"", i);
	        session.executeAsync(bound);
        }
        
        endtime = System.nanoTime();
        System.out.println("Duration INSERT (ms): "+ (endtime-starttime)/1000000);
        
        
        
        String cqlStatement = "SELECT * FROM test;";
        int x = 0;
        // read all entries from users table
        for(Row row : session.execute(cqlStatement)) {
        	x += 1;
        }
        
        System.out.println("anz="+x);

        
        System.out.println("Done");
        session.close();
        session.getCluster().close();
        
        System.exit(0);
        
    }
    
    
    


    
    
    
    
    
}
