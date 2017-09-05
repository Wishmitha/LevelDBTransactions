import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Created by wishmitha on 8/17/17.
 */


public class TransactionWithIntermediateStatesExample {

    /*

    This is a complex scenario with intermediate states (keys) which are stored in a HashMap wwithin the transaction itself
    as explained below.

    */

    public static DB db;

    private final static String QueueA = "queueA";
    private final static String QueueB = "queueB";

    private final static String QueueAMessage = "queueA.MESSAGE";
    private final static String QueueAMessageCount = "queueA.MESSAGE_COUNT";

    private final static String QueueBMessage = "queueB.MESSAGE";
    private final static String QueueBMessageCount = "queueB.MESSAGE_COUNT";

    private final static String NewMessage = "New Message";

    private static int queueAMessageCount;
    private static int queueBMessageCount;

    private static boolean error = false; // toggle this between true and false to switch between toggle and rollback.

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        db = factory.open(new File("queues"), options);

        // initialization

        db.put(QueueAMessageCount.getBytes(),"0".getBytes()); // Initially queue A and B are empty.
        db.put(QueueBMessageCount.getBytes(),"0".getBytes());

        /*

        Assume we want add new 10 messages randomly to queue A and B in a transaction. And we need to keep track the
        message counts of the each queues too.

        */

        Transaction tx = new Transaction(db); // transaction initialization

        try {

            for (int i=0; i<10; i++){

                if(new Random().nextInt(50)%2==0){

                    // if the random number is even, message is inserted to QueueA.

                    tx.put((QueueAMessage+Integer.toString(i)).getBytes(),NewMessage.getBytes());

                    if(asString(tx.getKey(QueueAMessageCount))==null){

                        /*

                        initially we want to get the count value from the database as the value is not yet being stored
                        in the transaction hash map.

                        */

                        queueAMessageCount = Integer.parseInt(asString(db.get(QueueAMessageCount.getBytes())));
                    }else{

                        // once stored we get the count value from transaction.

                        queueAMessageCount = Integer.parseInt(asString(tx.getKey(QueueAMessageCount)));
                    }

                    queueAMessageCount++;

                    // exception is throwed here.

                    if(error){
                        throw new Exception("CRASH!");
                    }

                    /*

                    Now we need to store message count. But we cannot write to the database because transaction can be
                    rollbacked and if the changes are written to the database, they cannot be reverted. Hence these are stored
                    in the HashMap which is implemented within the transaction itself.

                    */

                    tx.setKey(QueueAMessageCount,Integer.toString(queueAMessageCount).getBytes());

                }else {

                    // if the random number is odd, message is inserted to QueueB.

                    // Similar to previous implementation

                    tx.put((QueueBMessage+Integer.toString(i)).getBytes(),NewMessage.getBytes());

                    if(asString(tx.getKey(QueueBMessageCount))==null){

                        queueBMessageCount = Integer.parseInt(asString(db.get(QueueBMessageCount.getBytes())));

                    }else{

                        queueBMessageCount = Integer.parseInt(asString(tx.getKey(QueueBMessageCount)));

                    }

                    queueBMessageCount++;

                    tx.setKey(QueueBMessageCount,Integer.toString(queueBMessageCount).getBytes());

                }
            }

            //Now the transaction is committed

            tx.commit(db);

        }catch (Exception ex){

            // rollbacked if exception occured

            tx.rollback(db);

        }finally {

            // finally transaction is closed

            tx.close();

        }

        // Dispalay Results

        DBIterator it = db.iterator();
        it.seekToFirst();

        while (it.hasNext()){
            String key = asString(it.peekNext().getKey());
            System.out.println(key+ " - " +asString(db.get(key.getBytes())));
            it.next();
            db.delete(key.getBytes());
        }

        System.out.println("Is Transaction Committed : " + tx.isCommited);


    }

}
