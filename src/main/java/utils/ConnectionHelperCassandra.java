package utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public final class ConnectionHelperCassandra {
	
	public static Session getDBConnection() {
        String serverIP = "10.0.0.10";//"127.0.0.1";  // achtung: die IP Adressen können sich immer wieder mal ändern...
        String serverIP2 = "10.0.0.8";
        String keyspace = "tests";//"example";
        
        Cluster cluster = Cluster.builder()
        		.addContactPoint(serverIP)
        		.addContactPoint(serverIP2)
        		.build();
        Session session = cluster.connect(keyspace);
        
        return session;
	}

}
