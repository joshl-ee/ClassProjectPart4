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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JoinIterator extends Iterator{


    private Iterator outerIterator;
    private Iterator innerIterator;
    private Cursor cursor;
    private ComparisonPredicate predicate;
    private Set<String> attrNames;
    private Database db;
    private List<String> joinPath = new ArrayList<>();
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
        Transaction tx = FDBHelper.openTransaction(db);
        // Loop through outer iterator. For each iteration, loop through entire inner iterator. Need to create a copy constructor for this
        Record outerRecord;
        Record innerRecord;
        // Debug log:
        int count = 0;
        while (outerIterator.hasNext()) {
            outerRecord = outerIterator.next();
            // Reset inner operator during each iteration of outer table
            innerIterator.startFromBeginning();
            while (innerIterator.hasNext()) {
                innerRecord = innerIterator.next();
//                System.out.println("Count: " + count);
                // TODO: Join Logic. Add to join store if predicate succeeds
                if (doesRecordMatchPredicate(outerRecord, innerRecord)) {
                    count++;
                    System.out.println("Join! count: " + count);
                    addToJoinStore(outerRecord, innerRecord, tx);
                }

            }
        }
        FDBHelper.commitTransaction(tx);
        return StatusCode.SUCCESS;
    }

    // Checks whether two records should be joined by using predicated
    public boolean doesRecordMatchPredicate(Record outerRecord, Record innerRecord) {
        // Calculate value of outer record
        Object outerValue = null;
        if (predicate.getLeftHandSideAttrName() != null) {
            outerValue = outerRecord.getValueForGivenAttrName(predicate.getLeftHandSideAttrName());
        }

        // Calculate value of inner record
        Object innerValue = null;
        if (predicate.getLeftHandSideAttrName() != null) {
            innerValue = outerRecord.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
        }
        // Get RHS operator if it exists
        boolean rhsOperation = false;
        AlgebraicOperator rhsOperator = null;
        Object rhsValue = null;
        if (predicate.getRightHandSideOperator() != null) {
            rhsOperator = predicate.getRightHandSideOperator();
            rhsOperation = true;
            rhsValue = predicate.getRightHandSideValue();
        }

        // Check for same type
        if (outerRecord.getTypeForGivenAttrName(predicate.getLeftHandSideAttrName()) != innerRecord.getTypeForGivenAttrName(predicate.getRightHandSideAttrName())) {
            return false;
        }

        // Use type specific method to calculate the RHS value using added ComparisonUtil function
        // Then, use compareTwo methods to return if predicate is matched
        AttributeType recType = outerRecord.getTypeForGivenAttrName(predicate.getLeftHandSideAttrName());
        Object finalValue;
        if (recType == AttributeType.INT) {
            if (rhsOperation) {
                finalValue = ComparisonUtils.calculateINT(innerValue, rhsOperator, rhsValue);
            }
            else finalValue = innerValue;
            return ComparisonUtils.compareTwoINT(outerValue, finalValue, predicate.getOperator());
        } else if (recType == AttributeType.DOUBLE){
            if (rhsOperation) {
                finalValue = ComparisonUtils.calculateDOUBLE(innerValue, rhsOperator, rhsValue);
            }
            else finalValue = innerValue;
            return ComparisonUtils.compareTwoDOUBLE(outerValue, finalValue, predicate.getOperator());
        } else if (recType == AttributeType.VARCHAR) {
            System.out.println("This is not possible");
            return false;
        }
        return false;
    }

    // Add to join store. Called after predicate check succeeds.
    private StatusCode addToJoinStore(Record outerRecord, Record innerRecord, Transaction tx) {
        // TODO: Build joined KVPair
        Transaction addTx = FDBHelper.openTransaction(db);
//        FDBKVPair kvpair = new FDBKVPair(joinPath, new Tuple().addObject(value), new Tuple());
//        FDBHelper.setFDBKVPair(subspace, addTx, kvpair);
//        FDBHelper.commitTransaction(addTx);
        return StatusCode.SUCCESS;
    }

    @Override
    public Record next() {
        // Check if cursor is initialized
        Record record;
        if (!recorder.isInitialized(cursor)) record = recorder.getFirst(cursor);
        else record = recorder.getNext(cursor);

        return record;
    }

    @Override
    public boolean hasNext() {
        if (!cursor.isInitialized()) return true;
        return cursor.hasNext();
    }

    @Override
    public StatusCode startFromBeginning() {
        cursor = recorder.openCursor(joinTableName, Cursor.Mode.READ);
        return StatusCode.SUCCESS;
    }

    @Override
    public void commit() {
        // Delete Join Store
        Transaction tx = FDBHelper.openTransaction(db);
        FDBHelper.dropSubspace(tx, joinPath);
        FDBHelper.commitTransaction(tx);
    }

    @Override
    public void abort() {
        // Delete Join Store
        Transaction tx = FDBHelper.openTransaction(db);
        FDBHelper.dropSubspace(tx, joinPath);
        FDBHelper.commitTransaction(tx);
    }
}
