package testgroup.testartifact;

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
 * https://gist.github.com/rssvihla/4b62b8e5625a805583c1ce39b1260ff4#file-bulkloader-java
 * 
 * create table rows (id int primary key, value text);
 * 
 */ 
 public class Bulkloader2 {

private final int threads;
private final String[] contactHosts;

public Bulkloader2(int threads, String...contactHosts){
    this.threads = threads;
    this.contactHosts = contactHosts;
}

//callback class
public static class IngestCallback implements FutureCallback<ResultSet>{

    public void onSuccess(ResultSet result) {
        //placeholder: put any logging or on success logic here.
    }

    public void onFailure(Throwable t) {
        //go ahead and wrap in a runtime exception for this case, but you can do logging or start counting errors.
        throw new RuntimeException(t);
    }
}

public void ingest(Iterator<Object[]> boundItemsIterator, String insertCQL) throws InterruptedException {
    Cluster cluster = Cluster.builder()
            .addContactPoints(contactHosts)
            .build();
    Session session = cluster.newSession();
    //fixed thread pool that closes on app exit
    ExecutorService executor = MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor)Executors.newFixedThreadPool(threads));
    final PreparedStatement statement = session.prepare(insertCQL);
    while (boundItemsIterator.hasNext()) {
        BoundStatement boundStatement = statement.bind(boundItemsIterator.next());
        ResultSetFuture future = session.executeAsync(boundStatement);
        Futures.addCallback(future, new IngestCallback(), executor);
    }
    executor.shutdown();
    try {
       executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
     } catch (InterruptedException e) { /*dosomething*/ }
    session.close();
    cluster.close();
}

public static void main(String[] args) throws InterruptedException {
    Iterator<Object[]> rows = new Iterator<Object[]>() {
        int i = 0;
        Random random = new Random();

        public boolean hasNext() {
            //return i!=1000000;
        	return i!=10000;
        }

        public Object[] next() {
            i++;
            return new Object[]{i, String.valueOf(random.nextLong())};
        }
    };

    System.out.println("Starting benchmark");
    Stopwatch watch = Stopwatch.createStarted();
    new Bulkloader2(4, "10.0.0.14" /*, "10.0.0.11"*/).ingest(rows,
            "INSERT INTO tests.rows (id, value) VALUES (?,?) IF NOT EXISTS");
    System.out.println("total time seconds = " + watch.elapsed(TimeUnit.SECONDS));
    watch.stop();
    System.exit(0);
}
    }