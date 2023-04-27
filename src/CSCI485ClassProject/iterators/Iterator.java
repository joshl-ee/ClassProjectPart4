package CSCI485ClassProject.iterators;

import CSCI485ClassProject.Indexes;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.models.Record;

public abstract class Iterator {

  // For subclasses:
  // 1. Create index on attr being selected with predicate
  // 2. Instantiate/return cursor using enablePredicate on created index using

  // Cursor cursor;

  protected String tableName;
  protected Indexes indexer;
  protected Records recorder;
  protected Record currRecord;
  public enum Mode {
    READ,
    READ_WRITE
  }

  private Mode mode;

  public Mode getMode() {
    return mode;
  };

  public void setMode(Mode mode) {
    this.mode = mode;
  };
  public void getCurrent() { return this.currRecord; }
  public abstract Record next();
  public abstract boolean hasNext();
  public abstract StatusCode startFromBeginning();
  public abstract void commit();
  public String getTableName() {return this.tableName;}
  public abstract void abort();
}
