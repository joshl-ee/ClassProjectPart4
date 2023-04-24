package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.iterators.Iterator;
import CSCI485ClassProject.iterators.SelectIterator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.List;
import java.util.Set;

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
    if (predicate.validate() != StatusCode.PREDICATE_OR_EXPRESSION_VALID) {
      System.out.println("Predicate invalid");
      return null;
    }

    // Check if the table exists in table
    TableMetadata metadata = getTableMetadataByTableName(tx, tableName);
    if (!metadata.doesAttributeExist(predicate.getLeftHandSideAttrName()) || !metadata.doesAttributeExist(predicate.getRightHandSideAttrName())) {
      FDBHelper.abortTransaction(tx);
      System.out.println("Table does not exist");
      return null;
    }

    // Create iterator
    SelectIterator iterator = new SelectIterator(db, tableName, metadata, predicate, mode);

    // Check if EOF

    FDBHelper.commitTransaction(tx);
    return iterator;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    return null;
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return null;
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    return null;
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {
    return null;
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
