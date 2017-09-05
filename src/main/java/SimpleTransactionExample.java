import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * Created by wishmitha on 9/6/17.
 */
public class SimpleTransactionExample {

    /*

    This is a simple scenario where an one operation is carried out by a transaction.

    */

    public static DB db;

    private final static String AccountA = "Account:A";
    private final static String AccountB = "Account:B";

    private static int amountAccountA;
    private static int amountAccountB;

    private static int amountFromBtoA;

    private static boolean error = false; // toggle this between true and false to switch between toggle and rollback.

    public static void main(String[] args) throws IOException {

        Options options = new Options();
        db = factory.open(new File("accounts"), options);

        // initial amounts of A and B accounts

        db.put(AccountA.getBytes(),"1000".getBytes());
        db.put(AccountB.getBytes(),"3000".getBytes());

        // Initial amounts display

        System.out.println("Initial " + AccountA + " -" + asString(db.get(AccountA.getBytes())));
        System.out.println("Initial " + AccountB + " -" + asString(db.get(AccountB.getBytes())));

        /*

        Assume we want to send 1500 from B to A transactionally.

        Transaction:
            1) Send 1500 from B to A

         */

        amountFromBtoA = 1500;

        Transaction tx = new Transaction(db); // transaction initialization

        try {

            amountAccountA = Integer.parseInt(asString(db.get(AccountA.getBytes())));
            amountAccountB = Integer.parseInt(asString(db.get(AccountB.getBytes())));

            tx.put(AccountA.getBytes(),Integer.toString(amountAccountA+amountFromBtoA).getBytes());

            // exception is throwed here

            if(error){
                throw new Exception("CRASH!");
            }

            tx.put(AccountB.getBytes(),Integer.toString(amountAccountB-amountFromBtoA).getBytes());

            //Now the transaction is committed

            tx.commit(db);

        }catch (Exception ex){

            // rollbacked if exception occured

            tx.rollback(db);

        }finally {

            // finally transaction is closed

            tx.close();

        }

        // Final amounts display

        System.out.println("Final " + AccountA + " -" + asString(db.get(AccountA.getBytes())));
        System.out.println("Final " + AccountB + " -" + asString(db.get(AccountB.getBytes())));

        System.out.println("Is Transaction Committed : " + tx.isCommited);


    }
}
