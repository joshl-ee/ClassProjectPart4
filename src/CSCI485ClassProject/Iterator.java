package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;

public abstract class Iterator {

  // For subclasses:
  // 1. Create index on attr being selected with predicate
  // 2. Instantiate/return cursor using enablePredicate on created index using

  // Cursor cursor;

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

  public abstract Record next();

  public abstract void commit();

  public abstract void abort();
}
