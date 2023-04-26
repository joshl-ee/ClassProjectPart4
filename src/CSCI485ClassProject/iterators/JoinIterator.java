package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.IndexesImpl;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.AlgebraicOperator;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;
import java.util.Set;

public class JoinIterator extends Iterator{


    private Iterator outerIterator;
    private Iterator innerIterator;
    private Cursor cursor;
    private ComparisonPredicate predicate;
    private Set<String> attrNames;
    private Database db;
    private List<String> joinPath;
    private DirectorySubspace subspace;
    private String joinTableName;

    public JoinIterator(Database db, Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
        this.db = db;
        this.outerIterator = outerIterator;
        this.innerIterator = innerIterator;
        this.predicate = predicate;
        this.attrNames = attrNames;

        recorder = new RecordsImpl();
        indexer = new IndexesImpl(recorder);

        // Initialize join store
        if (initializeJoinStore() != StatusCode.SUCCESS) {
            System.out.println("Error creating join store");
        }
        // Populate join store
        else if (populateJoinStore() != StatusCode.SUCCESS) {
            System.out.println("Error populating join store");
        }

        // TODO: Create cursor on join store
        if (startFromBeginning() != StatusCode.SUCCESS) {
            System.out.println("Error initializing cursor");
        }
    }

    private StatusCode initializeJoinStore() {
        Transaction storeTx = FDBHelper.openTransaction(db);
        joinTableName = outerIterator.getTableName()+innerIterator.getTableName()+"Join";
        joinPath.add(joinTableName);
        subspace = FDBHelper.createOrOpenSubspace(storeTx, joinPath);
        FDBHelper.commitTransaction(storeTx);
        return StatusCode.SUCCESS;
    }


    // TODO: Populate join store
    private StatusCode populateJoinStore() {
        // Loop through outer iterator. For each iteration, loop through entire inner iterator. Need to create a copy constructor for this
        Record outerRecord;
        Record innerRecord;
        int count = 0;
        while (outerIterator.hasNext()) {
            // TODO: Implement resetOperation for Iterator
            outerRecord = outerIterator.next();
            innerIterator.startFromBeginning();
            while (innerIterator.hasNext()) {
                innerRecord = innerIterator.next();
                count++;
                System.out.println("Count: " + count);
                // TODO: Join Logic. Add to join store if predicate succeeds
                if (doesRecordMatchPredicate(outerRecord, innerRecord)) {
                    addToJoinStore(outerRecord, innerRecord);
                }

            }
        }

        return StatusCode.SUCCESS;
    }

    // TODO: Predicate checker
    public boolean doesRecordMatchPredicate(Record outerRecord, Record innerRecord) {

        return false;
    }

    // Add to join store. Called after predicate check succeeds.
    private StatusCode addToJoinStore(Record outerRecord, Record innerRecord) {
        // TODO: Build joined KVPair
        Transaction addTx = FDBHelper.openTransaction(db);
//        FDBKVPair kvpair = new FDBKVPair(joinPath, new Tuple().addObject(value), new Tuple());
//        FDBHelper.setFDBKVPair(subspace, addTx, kvpair);
//        FDBHelper.commitTransaction(addTx);
        return StatusCode.SUCCESS;
    }

    @Override
    public Record next() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public StatusCode startFromBeginning() {
        cursor = recorder.openCursor(joinTableName, Cursor.Mode.READ);
        return StatusCode.SUCCESS;
    }

    @Override
    public void commit() {
        // Delete Join Store

        // Abort tx
    }

    @Override
    public void abort() {
        // Delete Join Store

        // Abort tx
    }
}
