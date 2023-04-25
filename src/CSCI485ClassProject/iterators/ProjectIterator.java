package CSCI485ClassProject.iterators;

import CSCI485ClassProject.*;
import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class ProjectIterator extends Iterator{

    private String attrName;
    private Records recorder;
    private Cursor cursor = null;
    private Transaction tx;
    private boolean isUsingIterator;
    private boolean isDuplicateFree;
    private Iterator iterator = null;
    private List<String> duplicatePath = new ArrayList<>();
    private DirectorySubspace subspace = null;
    private Database db;

    public ProjectIterator(Database db, String tableName, String attrName, boolean isDuplicateFree) {
        tx = FDBHelper.openTransaction(db);
        this.tableName = tableName;
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;
        this.db = db;
        isUsingIterator = false;

        if (initializeDuplicateStore(attrName) != StatusCode.SUCCESS) {
            System.out.println("Error making duplicate store");
        }
        recorder = new RecordsImpl();

        cursor = recorder.openCursor(tableName, Cursor.Mode.READ);
    }

    public ProjectIterator(Database db, Iterator iterator, String attrName, boolean isDuplicateFree) {
        tx = FDBHelper.openTransaction(db);
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;
        this.db = db;
        isUsingIterator = true;

        if (initializeDuplicateStore(attrName) != StatusCode.SUCCESS) {
            System.out.println("Error making duplicate store");
        }

        this.iterator = iterator;
    }

    // TODO: If isDuplicateFree, create duplicate store
    private StatusCode initializeDuplicateStore(String attrName) {
        if (!isUsingIterator) duplicatePath.add(getTableName());
        else duplicatePath.add(iterator.getTableName());
        duplicatePath.add("Duplicates");
        duplicatePath.add(attrName);
        subspace = FDBHelper.createOrOpenSubspace(tx, duplicatePath);
        return StatusCode.SUCCESS;
    }

    // TODO: Add record to duplicate store if duplicate
    private StatusCode addToDuplicateStore(Record record) {
        Transaction addTx = FDBHelper.openTransaction(db);
        Object value = record.getValueForGivenAttrName(attrName);
        FDBKVPair kvpair = new FDBKVPair(duplicatePath, new Tuple().addObject(value), new Tuple());
        FDBHelper.setFDBKVPair(subspace, addTx, kvpair);
        FDBHelper.commitTransaction(addTx);
        return StatusCode.SUCCESS;
    }

    @Override
    public Record next() {
        Record record;
        if (!isUsingIterator && cursor != null) {
            if (!recorder.isInitialized(cursor)) record = project(recorder.getFirst(cursor));
            else record = project(recorder.getNext(cursor));
        }
        else {
            record = project(iterator.next());
        }

        // Check if record is duplicate
        if (isDuplicateFree) {
            if (!isUsingIterator) {
                while (cursor != null && isRecordDuplicated(record)) {
                    record = project(recorder.getNext(cursor));
                }
            }
            else {
                while (iterator != null && isRecordDuplicated(record)) {
                    record = project(iterator.next());
                }
            }
            // Add valid record to duplicate store
            if (addToDuplicateStore(record) != StatusCode.SUCCESS) {
                System.out.println("Error adding to duplicate store");
            }
        }

        return record;
    }

    @Override
    public boolean hasNext() {
        if (!isUsingIterator) {
            if (!cursor.isInitialized()) return true;
            return cursor.hasNext();
        }
        else {
            return iterator.hasNext();
        }
    }


    private Record project(Record record) {
        if (record == null) return null;
        Record projectedRecord = new Record();

        for (String attrName : record.getMapAttrNameToValue().keySet()) {
            if (this.attrName.equals(attrName)) projectedRecord.setAttrNameAndValue(attrName, record.getValueForGivenAttrName(attrName));
        }

        return projectedRecord;
    }

    private boolean isRecordDuplicated(Record record) {
        if (record == null) return false;
        Transaction searchTx = FDBHelper.openTransaction(db);
        Object value = record.getValueForGivenAttrName(attrName);
        if (FDBHelper.getCertainKeyValuePairInSubdirectory(subspace, searchTx, new Tuple().addObject(value), duplicatePath) != null) {
            FDBHelper.commitTransaction(searchTx);
            return true;
        }
        FDBHelper.commitTransaction(searchTx);
        return false;
    }

    @Override
    public void commit() {
        // Delete duplicate store structure

        // Commit tx

        return;
    }

    @Override
    public void abort() {
        // Delete duplicate store structure

        // Abort tx

        return;
    }
}
