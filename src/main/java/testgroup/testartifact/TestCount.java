package testgroup.testartifact;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import utils.ConnectionHelperCassandra;

public class TestCount {
	
	
    public static void main( String[] args ) throws InterruptedException
    {
        System.out.println( "Hello World!!!" );
        long starttime;
        long endtime;

		Session session = ConnectionHelperCassandra.getDBConnection();

	    //String cqlStatement = "SELECT * FROM tests.orders";
		String cqlStatement = "SELECT * FROM tests.orderline";
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
