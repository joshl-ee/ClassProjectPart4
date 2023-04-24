package CSCI485ClassProject.iterators;

import CSCI485ClassProject.*;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.iterators.Iterator;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

public class SelectIterator extends Iterator {

    String tableName;
    ComparisonPredicate predicate;
    Iterator.Mode mode;
    boolean isUsingIndex;
    Indexes indexer;
    Records recorder;
    Cursor cursor;
    Transaction tx;
    public SelectIterator(Database db, String tableName, TableMetadata metadata, ComparisonPredicate predicate, Iterator.Mode mode) {
        tx = FDBHelper.openTransaction(db);
        this.tableName = tableName;
        this.predicate = predicate;
        //this.mode = mode;
        recorder = new RecordsImpl();
        indexer = new IndexesImpl(recorder);

        // Create index
        indexer.createIndex(tableName, predicate.getLeftHandSideAttrName(), IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX);

        // Initialize cursor based on predicate
        Cursor.Mode cursorMode = mode == Iterator.Mode.READ ? Cursor.Mode.READ : Cursor.Mode.READ_WRITE;
        cursor = recorder.openCursor(tableName, predicate.getLeftHandSideAttrName(), predicate.getRightHandSideValue(), predicate.getOperator(), cursorMode, true);
    }

    @Override
    public Record next() {
        // Check if cursor is initialized
        if (!recorder.isInitialized(cursor)) return recorder.getFirst(cursor);
        return recorder.getNext(cursor);
    }

    @Override
    public void commit() {
        // Save changes
        FDBHelper.commitTransaction(tx);

        // Drop index
        indexer.dropIndex(tableName, predicate.getLeftHandSideAttrName());
        indexer.closeDatabase();
        recorder.closeDatabase();
    }

    @Override
    public void abort() {
        // Abort changes
        FDBHelper.abortTransaction(tx);

        // Drop index
        indexer.dropIndex(tableName, predicate.getLeftHandSideAttrName());
        indexer.closeDatabase();
        recorder.closeDatabase();
    }
}
