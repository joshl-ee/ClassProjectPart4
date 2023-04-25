package CSCI485ClassProject.iterators;

import CSCI485ClassProject.*;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import com.apple.foundationdb.Transaction;

public class ProjectIterator extends Iterator{

    private String tableName;
    private String attrName;
    private Records recorder;
    private Cursor cursor = null;
    private Transaction tx;
    private boolean isUsingIterator;
    private boolean isDuplicateFree;
    private Iterator iterator = null;

    public ProjectIterator(String tableName, String attrName, boolean isDuplicateFree) {
        this.tableName = tableName;
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;

        // If duplicate
        isUsingIterator = false;
        recorder = new RecordsImpl();

        cursor = recorder.openCursor(tableName, Cursor.Mode.READ);
    }

    public ProjectIterator(Iterator iterator, String attrName, boolean isDuplicateFree) {
        this.attrName = attrName;
        this.isDuplicateFree = isDuplicateFree;

        isUsingIterator = true;

        this.iterator = iterator;
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
        Record projectedRecord = new Record();

        for (String attrName : record.getMapAttrNameToValue().keySet()) {
            if (this.attrName.equals(attrName)) projectedRecord.setAttrNameAndValue(attrName, record.getValueForGivenAttrName(attrName));
        }

        return projectedRecord;
    }

    private boolean isRecordDuplicated(Record record) {

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
