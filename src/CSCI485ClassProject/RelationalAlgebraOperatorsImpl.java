package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.iterators.Iterator;
import CSCI485ClassProject.iterators.JoinIterator;
import CSCI485ClassProject.iterators.ProjectIterator;
import CSCI485ClassProject.iterators.SelectIterator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.*;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  Database db;

  public RelationalAlgebraOperatorsImpl() {
    db = FDBHelper.initialization();
  }

  private TableMetadata getTableMetadataByTableName(Transaction tx, String tableName) {
    TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tx,
            tblMetadataTransformer.getTableAttributeStorePath());
    TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
    return tblMetadata;
  }

  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    Transaction tx = FDBHelper.openTransaction(db);

    // Check if predicate is valid
    if (predicate.getPredicateType() == ComparisonPredicate.Type.TWO_ATTRS && predicate.getLeftHandSideAttrType() != predicate.getRightHandSideAttrType()) return null;
    if (predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      System.out.println("Predicate invalid");
      return null;
    }

    // Check if table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Table does not exist");
      return null;
    }

    // Check if the attr exists in table
    TableMetadata metadata = getTableMetadataByTableName(tx, tableName);

    if ((predicate.getLeftHandSideAttrName() != null && !metadata.doesAttributeExist(predicate.getLeftHandSideAttrName())) || (predicate.getRightHandSideAttrName() != null && !metadata.doesAttributeExist(predicate.getRightHandSideAttrName()))) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Attribute does not exist on table");
      return null;
    }

    // Create iterator
    SelectIterator iterator = new SelectIterator(db, tableName, predicate, mode, isUsingIndex);

    FDBHelper.commitTransaction(tx);
    return iterator;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    Set<Record> recordSet = new HashSet<>();
    Iterator iterator = this.select(tableName, predicate, Iterator.Mode.READ, isUsingIndex);

    while (iterator != null && iterator.hasNext()) {
      Record record = iterator.next();
      if (record != null) {
//        System.out.println("Record's salary: " + record.getValueForGivenAttrName("Salary"));
//        System.out.println("Records age*2: " + (long) record.getValueForGivenAttrName("Age")*2);
        recordSet.add(record);
      }
    }

    if (iterator != null) iterator.commit();
    return recordSet;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    Transaction tx = FDBHelper.openTransaction(db);

    // Check if table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Table does not exist");
      return null;
    }

    // Check if the attr exists in table
    TableMetadata metadata = getTableMetadataByTableName(tx, tableName);
    if (!metadata.doesAttributeExist(attrName)) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Attribute does not exist on table");
      return null;
    }

    ProjectIterator iterator = new ProjectIterator(db, tableName, attrName, isDuplicateFree, false);

    FDBHelper.commitTransaction(tx);
    return iterator;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    Transaction tx = FDBHelper.openTransaction(db);

    ProjectIterator p_iterator = new ProjectIterator(db, iterator, attrName, isDuplicateFree);

    FDBHelper.commitTransaction(tx);
    return p_iterator;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    List<Record> recordSet = new ArrayList<>();

    Transaction tx = FDBHelper.openTransaction(db);

    // Check if table exists
    if (!FDBHelper.doesSubdirectoryExists(tx, Collections.singletonList(tableName))) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Table does not exist");
      return null;
    }

    // Check if the attr exists in table
    TableMetadata metadata = getTableMetadataByTableName(tx, tableName);
    if (!metadata.doesAttributeExist(attrName)) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Attribute does not exist on table");
      return null;
    }

    ProjectIterator iterator = new ProjectIterator(db, tableName, attrName, isDuplicateFree, true);

    FDBHelper.commitTransaction(tx);


    while (iterator.hasNext()) {
      Record record = iterator.next();
      if (record != null) {
        //System.out.println("Record's DNO: " + record.getValueForGivenAttrName("DNO"));
        recordSet.add(record);
      }
      else break;
    }

    iterator.commit();
    return recordSet;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    List<Record> recordSet = new ArrayList<>();
    Iterator p_iterator = this.project(iterator, attrName, isDuplicateFree);

    while (p_iterator != null && p_iterator.hasNext()) {
      Record record = p_iterator.next();
      if (record != null) {
//        System.out.println("Record's salary: " + record.getValueForGivenAttrName("Salary"));
//        System.out.println("Records age*2: " + (long) record.getValueForGivenAttrName("Age")*2);
        recordSet.add(record);
      }
    }

    p_iterator.commit();
    return recordSet;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    Transaction tx = FDBHelper.openTransaction(db);

    JoinIterator iterator = new JoinIterator(db, outerIterator, innerIterator, predicate, attrNames);

    FDBHelper.commitTransaction(tx);

    return iterator;
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {

    Records recorder = new RecordsImpl();

    List<Object> primaryKeyValues = new ArrayList<>();
    List<String> attrNames = new ArrayList<>();
    List<Object> attrValues = new ArrayList<>();

    for (String attrName : record.getMapAttrNameToValue().keySet()) {
      boolean primaryKey = false;
      for (int i = 0; i < primaryKeys.length; i++) {
        if (attrName.equals(primaryKeys[i])) {
          primaryKeyValues.add(record.getValueForGivenAttrName(attrName));
          primaryKey = true;
          break;
        }
      }
      if (!primaryKey) {
        attrNames.add(attrName);
        attrValues.add(record.getValueForGivenAttrName(attrName));
      }
    }


    return recorder.insertRecord(tableName, primaryKeys, primaryKeyValues.toArray(), attrNames.toArray(new String[attrNames.size()]), attrValues.toArray());

  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {
    return null;
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    return null;
  }

  @Override
  public StatusCode closeDatabase() {
    FDBHelper.close(db);
    return StatusCode.SUCCESS;
  }
}
