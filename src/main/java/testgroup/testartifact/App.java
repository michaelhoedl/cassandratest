package testgroup.testartifact;

import java.util.Date;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!!" );
        
        String serverIP = "127.0.0.1";
        String keyspace = "example";
        
        Cluster cluster = Cluster.builder()
        		.addContactPoint(serverIP)
        		.build();
        Session session = cluster.connect(keyspace);
        
        String cqlStatement = "SELECT json * FROM users;";
        
        // read all entries from users table
        //for(Row row : session.execute(cqlStatement)) {
        //	System.out.println(row.toString());
        //}
        
        
        long starttime = System.nanoTime();
        
        // add new users
        for(int i = 1; i <= 10000; i++) {
	        String cqlInsertStatement = "INSERT INTO users (id, name) "
	        		+ "VALUES ('user"+i+"', 'john"+i+"');";
	
	        session.execute(cqlInsertStatement);
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
