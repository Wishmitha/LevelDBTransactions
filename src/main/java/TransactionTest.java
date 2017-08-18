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

        boolean error = true;

        db.put("Account:A".getBytes(),"1000".getBytes());
        db.put("Account:B".getBytes(),"3000".getBytes());

        System.out.println("Initial Account:A - "+asString(db.get("Account:A".getBytes())));
        System.out.println("Initial Account:B - "+asString(db.get("Account:B".getBytes())));

        // B sends 500 to A's account from his account
        int amount =500;

        Transaction t1 = new Transaction(db);

        try {

            int initalB = Integer.parseInt(asString(db.get("Account:B".getBytes())));
            int newB = initalB - amount;

            t1.put("Account:B".getBytes(),Integer.toString(newB).getBytes());

            if(!error){ // TODO toggle error and check change in results
                throw new Exception("CRASH!");
            }

            int initialA = Integer.parseInt(asString(db.get("Account:A".getBytes())));
            int newA = initialA + amount;

            t1.put("Account:A".getBytes(),Integer.toString(newA).getBytes());

            t1.commit(db);

        }catch (Exception ex){

            t1.rollback(db);

        }

        t1.close();

        System.out.println("New Account:A - "+asString(db.get("Account:A".getBytes())));
        System.out.println("New Account:B - "+asString(db.get("Account:B".getBytes())));

        System.out.println("Is Transaction Committed : " + t1.isCommited);

        Transaction t2 = new Transaction(db);

        try {

            t2.delete("Account:A".getBytes());
            t2.delete("Account:B".getBytes());

            t2.commit(db);

        }catch (Exception ex){

            t2.rollback(db);

        }

        t2.close();


    }

}
