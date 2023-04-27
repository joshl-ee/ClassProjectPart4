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

import java.util.*;

public class JoinIterator extends Iterator{

    private Iterator outerIterator;
    private Iterator innerIterator;
    private ComparisonPredicate predicate;
    private Set<String> attrNames = null;
    private Database db;
    private Record currOuterRecord;
    private Record currInnerRecord;
    private Transaction tx;
    private HashMap<String, String> outerNameUpdate = new HashMap<>();
    private HashMap<String, String> innerNameUpdate = new HashMap<>();
    private boolean attrSetProvided = false;

    public JoinIterator(Database db, Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
        this.db = db;
        this.outerIterator = outerIterator;
        this.innerIterator = innerIterator;
        this.predicate = predicate;
        this.attrNames = attrNames;
        if (attrNames != null && attrNames.size() > 0) attrSetProvided = true;

        recorder = new RecordsImpl();
        indexer = new IndexesImpl(recorder);
        this.tx = FDBHelper.openTransaction(db);

        // Check for duplicated attribute names and rename them.
        String outerTableName = outerIterator.getTableName();
        String innerTableName = innerIterator.getTableName();
        TableMetadata outerMetadata = getTableMetadataByTableName(tx, outerTableName);
        TableMetadata innerMetadata = getTableMetadataByTableName(tx, innerTableName);

        for (String outerName : outerMetadata.getAttributes().keySet()) {
            for (String innerName: innerMetadata.getAttributes().keySet()) {
                if (outerName.equals(innerName)) {
                    // If same, rename attributes
                    String newOuterAttrName = outerTableName+".poo"+outerName;
                    String newInnerAttrName = innerTableName+"."+innerName;
                    outerNameUpdate.put(outerName, newOuterAttrName);
                    innerNameUpdate.put(innerName, newInnerAttrName);
                }
            }
        }

        if (startFromBeginning() != StatusCode.SUCCESS) {
            System.out.println("Error initializing cursor");
        }
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
        if (outerRecord == null || innerRecord == null) return false;

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

    // Build the joined record once a match is found
    private Record buildJoinedRecord(Record outerRecord, Record innerRecord) {
        Record joinedRecord = new Record();

        for (String attrName : outerRecord.getMapAttrNameToValue().keySet()) {
            joinedRecord.setAttrNameAndValue(outerNameUpdate.getOrDefault(attrName, attrName), outerRecord.getValueForGivenAttrName(attrName));
        }
        for (String attrName : innerRecord.getMapAttrNameToValue().keySet()) {
            joinedRecord.setAttrNameAndValue(innerNameUpdate.getOrDefault(attrName, attrName), innerRecord.getValueForGivenAttrName(attrName));

        }

        return joinedRecord;
    }

    @Override
    public Record next() {
        Boolean found = false;

        // TODO: Find next two records to join
        while (!found) {
            if (innerIterator.hasNext()) {
                currInnerRecord = innerIterator.next();
                if (doesRecordMatchPredicate(currOuterRecord, currInnerRecord)) {
                    found = true;
                }
            }
            else if (outerIterator.hasNext()){
                innerIterator.startFromBeginning();
                currOuterRecord = outerIterator.next();
                if (doesRecordMatchPredicate(currOuterRecord, currInnerRecord)) {
                    found = true;
                }
            }
            else {
                return null;
            }
        }

        // If found: create joined record
        return buildJoinedRecord(currOuterRecord, currInnerRecord);
    }

    @Override
    public boolean hasNext() {
        return outerIterator.hasNext() || innerIterator.hasNext();
    }

    @Override
    public StatusCode startFromBeginning() {
        outerIterator.startFromBeginning();
        innerIterator.startFromBeginning();

        return StatusCode.SUCCESS;
    }

    @Override
    public void commit() {
        // Delete Join Store
        Transaction tx = FDBHelper.openTransaction(db);
        FDBHelper.commitTransaction(tx);
    }

    @Override
    public void abort() {
        // Delete Join Store
        Transaction tx = FDBHelper.openTransaction(db);
        FDBHelper.commitTransaction(tx);
    }
}
