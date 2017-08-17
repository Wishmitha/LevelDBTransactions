import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Created by wishmitha on 8/17/17.
 */


public class TransactionTest {

    public static DB db;

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        db = factory.open(new File("ldb"), options);

        ArrayList<Transaction> transactions = new ArrayList<Transaction>();

        // Transaction t1

        Transaction t1 = new Transaction(db);
        transactions.add(t1);

        try {
            t1.put("a.1".getBytes(),"a1".getBytes());
            t1.put("a.2".getBytes(),"a2".getBytes());
            t1.put("a.3".getBytes(),"a3".getBytes());

            if(false){
                throw new Exception("lol");
            }

            t1.commit(db);

        }catch (Exception ex){

            t1.rollback(db);
        }

        t1.close();

        // Transaction t2

        Transaction t2 = new Transaction(db);
        transactions.add(t2);

        try {
            t2.put("b.1".getBytes(),"b1".getBytes());
            t2.put("b.2".getBytes(),"b2".getBytes());
            t2.put("b.3".getBytes(),"b3".getBytes());

            if(false){
                throw new Exception("lol");
            }

            t2.commit(db);

        }catch (Exception ex){

            t2.rollback(db);

        }

        t2.close();

        // Transaction t3

        Transaction t3 = new Transaction(db);
        transactions.add(t3);

        try {
            t3.put("a.1".getBytes(),"c1".getBytes());
            t3.put("a.2".getBytes(),"c2".getBytes());
            t3.put("a.3".getBytes(),"c3".getBytes());

            if(false){
                throw new Exception("lol");
            }


            t3.commit(db);

        }catch (Exception ex){

            t3.rollback(db);

        }

        t3.close();

        Transaction t4 = new Transaction(db);
        transactions.add(t4);

        try {
            t4.delete("b.1".getBytes());
            t4.delete("b.2".getBytes());
            t4.delete("b.3".getBytes());

            if(true){
                throw new Exception("lol");
            }


            t4.commit(db);

        }catch (Exception ex){

            t4.rollback(db);

        }

        t4.close();

        DBIterator it = db.iterator();
        it.seekToFirst();

        while (it.hasNext()){
            String key = asString(it.peekNext().getKey());
            System.out.print(key + " : ");

            Transaction t5 = new Transaction(db);

            System.out.println(asString(t5.get(key.getBytes())));
            it.next();
        }
    }

}
