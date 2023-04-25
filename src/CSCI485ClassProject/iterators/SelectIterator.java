package CSCI485ClassProject.iterators;

import CSCI485ClassProject.*;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

public class SelectIterator extends Iterator {

    private String tableName;
    private ComparisonPredicate predicate;
    private Iterator.Mode mode;
    private boolean isUsingIndex;
    private Indexes indexer;
    private Records recorder;
    private Cursor cursor;
    private Transaction tx;

    public SelectIterator(Database db, String tableName, TableMetadata metadata, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
        tx = FDBHelper.openTransaction(db);
        this.tableName = tableName;
        this.predicate = predicate;
        this.isUsingIndex = isUsingIndex;
        //this.mode = mode;
        recorder = new RecordsImpl();
        indexer = new IndexesImpl(recorder);

        // Create index
        if (isUsingIndex) indexer.createIndex(tableName, predicate.getLeftHandSideAttrName(), IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX);

        // Initialize cursor based on predicate
        Cursor.Mode cursorMode = mode == Iterator.Mode.READ ? Cursor.Mode.READ : Cursor.Mode.READ_WRITE;
        System.out.println("LeftHandSideAttrName: " + predicate.getLeftHandSideAttrName());
        System.out.println("RightHandSideValue: " + predicate.getRightHandSideValue());
        System.out.println("Operator: " + predicate.getOperator());

        if (predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR) {
            cursor = recorder.openCursor(tableName, predicate.getLeftHandSideAttrName(), predicate.getRightHandSideValue(), predicate.getOperator(), cursorMode, isUsingIndex);
        }
        else {
            // TODO: Check if this will be a problem. Might be since we CANNOT use indicies when predicate type is TWO_ATTR
            // TODO: Check if attributes are comparable AKA if types are the same
            cursor = recorder.openCursor(tableName, cursorMode);
        }
    }

    @Override
    public Record next() {
        // Check if cursor is initialized
        Record record;
        if (!recorder.isInitialized(cursor)) record = recorder.getFirst(cursor);
        record = recorder.getNext(cursor);
        while (predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS && !doesRecordMatchPredicate(record)) record = recorder.getNext(cursor);
        return record;
    }

    public boolean doesRecordMatchPredicate(Record record) {
        // Calculate value of right side
        Object attrValue = record.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
        AlgebraicOperator rhsOperator = predicate.getRightHandSideOperator();
        Object rhsValue = predicate.getRightHandSideValue();

        AttributeType recType = record.getTypeForGivenAttrName(predicate.getRightHandSideAttrName());
        Object finalValue;
        if (recType == AttributeType.INT) {
            finalValue = ComparisonUtils.calculateINT(attrValue, rhsOperator, rhsValue);
            return ComparisonUtils.compareTwoINT(record.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), finalValue, predicate.getOperator());
        } else if (recType == AttributeType.DOUBLE){
            finalValue = ComparisonUtils.calculateDOUBLE(attrValue, rhsOperator, rhsValue);
            return ComparisonUtils.compareTwoDOUBLE(record.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), finalValue, predicate.getOperator());
        } else if (recType == AttributeType.VARCHAR) {
            finalValue = ComparisonUtils.calculateVARCHAR(attrValue, rhsOperator, rhsValue);
            return ComparisonUtils.compareTwoVARCHAR(record.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), finalValue, predicate.getOperator());
        }
        return false;
    }

    @Override
    public void commit() {
        // Save changes
        FDBHelper.commitTransaction(tx);

        // Drop index
        if (isUsingIndex) indexer.dropIndex(tableName, predicate.getLeftHandSideAttrName());
        indexer.closeDatabase();
        recorder.closeDatabase();
    }

    @Override
    public boolean hasNext() {
        if (!cursor.isInitialized()) return true;
        return cursor.hasNext();
    }

    @Override
    public void abort() {
        // Abort changes
        FDBHelper.abortTransaction(tx);

        // Drop index
        if (isUsingIndex) indexer.dropIndex(tableName, predicate.getLeftHandSideAttrName());
        indexer.closeDatabase();
        recorder.closeDatabase();
    }
}
