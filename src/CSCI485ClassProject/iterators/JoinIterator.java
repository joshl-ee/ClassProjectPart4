package CSCI485ClassProject.iterators;

import CSCI485ClassProject.*;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
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

        if (startFromBeginning() != StatusCode.SUCCESS) {
            System.out.println("Error initializing cursor");
        }
    }

    private StatusCode initializeJoinStore() {
        Transaction storeTx = FDBHelper.openTransaction(db);
        joinTableName = outerIterator.getTableName()+innerIterator.getTableName()+"Join";
        joinPath.add(joinTableName);
        joinPath.add("records");
        subspace = FDBHelper.createOrOpenSubspace(storeTx, joinPath);
        FDBHelper.commitTransaction(storeTx);
        return StatusCode.SUCCESS;
    }


    // TODO: Populate join store
    private StatusCode populateJoinStore() {
        Transaction tx = FDBHelper.openTransaction(db);

        // Check for duplicated attribute names and rename them.
        String outerTableName = outerIterator.getTableName();
        String innerTableName = innerIterator.getTableName();
        TableMetadata outerMetadata = getTableMetadataByTableName(tx, outerTableName);
        TableMetadata innerMetadata = getTableMetadataByTableName(tx, innerTableName);
        HashMap<String, String> outerNameUpdate = new HashMap<>();
        HashMap<String, String> innerNameUpdate = new HashMap<>();

        for (String outerName : outerMetadata.getAttributes().keySet()) {
            for (String innerName: innerMetadata.getAttributes().keySet()) {
                if (outerName.equals(innerName)) {
                    // If same, rename attributes
                    String newOuterAttrName = outerTableName+"."+outerName;
                    String newInnerAttrName = innerTableName+"."+innerName;
                    outerNameUpdate.put(outerName, newOuterAttrName);
                    innerNameUpdate.put(innerName, newInnerAttrName);
                }
            }
        }

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
                    if (addToJoinStore(outerRecord, innerRecord, tx, outerNameUpdate, innerNameUpdate) != StatusCode.SUCCESS) {
                        System.out.println("Failed to add to join records");
                    }
                }

            }
        }
        FDBHelper.commitTransaction(tx);
        return StatusCode.SUCCESS;
    }

    private TableMetadata getTableMetadataByTableName(Transaction tx, String tableName) {
        TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
        List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx,
                tblMetadataTransformer.getTableAttributeStorePath());
        TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
        return tblMetadata;
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
            innerValue = innerRecord.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
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

    // TODO: Build joined KVPair
    // Add to join store. Called after predicate check succeeds.
    private StatusCode addToJoinStore(Record outerRecord, Record innerRecord, Transaction tx, HashMap<String, String> outerNameUpdate, HashMap<String, String> innerNameUpdate) {
        Record joinedRecord = new Record();

        // TODO: Add joined record to database
        for (String attrName : outerRecord.getMapAttrNameToValue().keySet()) {
            joinedRecord.setAttrNameAndValue(outerNameUpdate.getOrDefault(attrName, attrName), outerRecord.getValueForGivenAttrName(attrName));
        }
        for (String attrName : innerRecord.getMapAttrNameToValue().keySet()) {
            joinedRecord.setAttrNameAndValue(innerNameUpdate.getOrDefault(attrName, attrName), innerRecord.getValueForGivenAttrName(attrName));

        }

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
