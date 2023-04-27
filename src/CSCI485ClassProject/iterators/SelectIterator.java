package CSCI485ClassProject.iterators;

import CSCI485ClassProject.*;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

public class SelectIterator extends Iterator {

    private ComparisonPredicate predicate;
    private Iterator.Mode mode;
    private boolean isUsingIndex;
    private Cursor cursor;
    private Transaction tx;
    private Cursor.Mode cursorMode;

    public SelectIterator(Database db, String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
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
        cursorMode = mode == Iterator.Mode.READ ? Cursor.Mode.READ : Cursor.Mode.READ_WRITE;
//        System.out.println("LeftHandSideAttrName: " + predicate.getLeftHandSideAttrName());
//        System.out.println("RightHandSideValue: " + predicate.getRightHandSideValue());
//        System.out.println("Operator: " + predicate.getOperator());

        // Create cursor depending on whether there is ONE_ATTR or TWO_ATTR in the predicate.
        // If there is one, we can just use index structure. Otherwise, checking for predicate is more complicated
        if (startFromBeginning() != StatusCode.SUCCESS) {
            System.out.println("Error initializing cursor");
        }
    }

    @Override
    public Record next() {
        // Check if cursor is initialized
        Record record;
        if (!recorder.isInitialized(cursor)) record = recorder.getFirst(cursor);
        else record = recorder.getNext(cursor);
        while (record != null && predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS && !doesRecordMatchPredicate(record)) {
//            System.out.println("Skip!: " + skip);
            record = recorder.getNext(cursor);
        }
        currRecord = record;
        return record;
    }

    // Predicate checker for TWO_ATTR
    public boolean doesRecordMatchPredicate(Record record) {
        // Calculate value of right side of current record
        Object attrValue = record.getValueForGivenAttrName(predicate.getRightHandSideAttrName());
        AlgebraicOperator rhsOperator = predicate.getRightHandSideOperator();
        Object rhsValue = predicate.getRightHandSideValue();

        // Given attribute type, use a specific method to calculate the RHS value using added ComparisonUtil function
        // Then, use compareTwo methods to return if predicate is matched
        AttributeType recType = record.getTypeForGivenAttrName(predicate.getRightHandSideAttrName());
        Object finalValue;
        if (recType == AttributeType.INT) {
            finalValue = ComparisonUtils.calculateINT(attrValue, rhsOperator, rhsValue);
            return ComparisonUtils.compareTwoINT(record.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), finalValue, predicate.getOperator());
        } else if (recType == AttributeType.DOUBLE){
            finalValue = ComparisonUtils.calculateDOUBLE(attrValue, rhsOperator, rhsValue);
            return ComparisonUtils.compareTwoDOUBLE(record.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), finalValue, predicate.getOperator());
        } else if (recType == AttributeType.VARCHAR) {
            System.out.println("This is not possible");
            return false;
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

    // Method to start from beginning
    @Override
    public StatusCode startFromBeginning() {
        if (predicate.getPredicateType() == ComparisonPredicate.Type.ONE_ATTR) {
            cursor = recorder.openCursor(tableName, predicate.getLeftHandSideAttrName(), predicate.getRightHandSideValue(), predicate.getOperator(), cursorMode, isUsingIndex);
        }
        else {
            cursor = recorder.openCursor(tableName, cursorMode);
        }
        return StatusCode.SUCCESS;
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
