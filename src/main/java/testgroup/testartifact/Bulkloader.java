package testgroup.testartifact;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * I HAVE NOT BEEN FULLY TESTED DO NOT USE ME IN PRODUCTION CODE THERE ARE NO WARRANTIES
 * https://gist.github.com/rssvihla/26271f351bdd679553d55368171407be#file-bulkloader-java
 * 
 * create table rows (id int primary key, value text);
 * 
 */ 
 public class Bulkloader {

private final int threads;
private final String[] contactHosts;

public Bulkloader(int threads, String...contactHosts){
    this.threads = threads;
    this.contactHosts = contactHosts;
}

public void ingest(Iterator<Object[]> boundItemsIterator, String insertCQL) throws InterruptedException {
    Cluster cluster = Cluster.builder()
            .addContactPoints(contactHosts)
            .build();
    Session session = cluster.newSession();
    //there are better ways to do this using executor pools
    ArrayList<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
    final PreparedStatement statement = session.prepare(insertCQL);
    int count = 0;
    while (boundItemsIterator.hasNext()) {
        BoundStatement boundStatement = statement.bind(boundItemsIterator.next());
        ResultSetFuture future = session.executeAsync(boundStatement);
        futures.add(future);
        count++;
        if(count % threads==0){
            futures.forEach(ResultSetFuture::getUninterruptibly);
            futures = new ArrayList<ResultSetFuture>();
        }
    }
    session.close();
    cluster.close();
}


public static void main(String[] args) throws InterruptedException {
    Iterator<Object[]> rows = new Iterator<Object[]>() {
        int i = 0;
        Random random = new Random();

        public boolean hasNext() {
            return i!=1000000;
        }

        public Object[] next() {
            i++;
            return new Object[]{i, String.valueOf(random.nextLong())};
        }
    };

    System.out.println("Starting benchmark");
Stopwatch watch = Stopwatch.createStarted();
    new Bulkloader(4, "10.0.0.14" /*, "10.0.0.11"*/).ingest(rows,
            "INSERT INTO tests.rows (id, value) VALUES (?,?) IF NOT EXISTS");
    System.out.println("total time (ms) = " + watch.elapsed(TimeUnit.MILLISECONDS));
    watch.stop();
    System.exit(0);
}
    }