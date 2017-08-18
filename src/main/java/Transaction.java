import org.iq80.leveldb.DB;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;

import java.io.IOException;

/**
 * Created by wishmitha on 8/17/17.
 */
public class Transaction {

    WriteBatch batch;
    boolean isCommited;

    public Transaction(DB db){
        this.batch = db.createWriteBatch();
    }

    public void close() throws IOException {
        this.batch.close();
    }

    public void commit(DB db){
        db.write(this.batch);
        this.isCommited = true;
    }

    public void rollback(DB db) {
        this.batch = db.createWriteBatch();
        this.isCommited = false;
    }

    public void put(byte[] key, byte[] value){
        this.batch.put(key,value);
    }

    public void delete(byte[] key){
        this.batch.delete(key);
    }

}
