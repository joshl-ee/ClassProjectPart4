package CSCI485ClassProject;

import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.Record;

import java.util.List;
import java.util.Set;

public interface RelationalAlgebraOperators {

  /**
   * Provide an iterator that fetches records that satisfy a selection predicate.
   * @param tableName the target table name
   * @param predicate the predicate to filter records
   * @param mode the mode of the iterator
   * @param isUsingIndex if true, records need to be retrieved using the index
   * @return iterator that specify the qualified records.
   */
  Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex);

  /**
   * Return a set of records that satisfy a selection predicate.
   * @param tableName the target table name
   * @param predicate the predicate to filter records
   * @param isUsingIndex if true, records need to be retrieved using the index
   * @return a set of qualified records
   */
  Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex);

  /**
   * Project specified attribute of a record from the table. Only one attribute is specified.
   * @param tableName the target table name
   * @param attrName the attribute name to be projected
   * @param isDuplicateFree if true. The attribute value will be duplication-free and follows the ASCENDING order.
   * @return iterator that points to the Record with only the given attribute
   */
  Iterator project(String tableName, String attrName, boolean isDuplicateFree);

  /**
   * Project specified attribute of a record from another iterator. Only one attribute is specified.
   * @param iterator the target table name
   * @param attrName the attribute name to be projected
   * @param isDuplicateFree if true. The attribute value will be duplication-free and follows the ASCENDING order.
   * @return iterator that points to the Record with only the given attribute
   */
  Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree);

  /**
   * Returns a set of records that project only the given attributeName from a table
   */
  List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree);

  /**
   * Returns a set of records that project only the given attributeName from another iterator
   */
  List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree);

  /**
   * Join two iterators together on the given predicate. A set of attribute names may be provided.
   *
   * If a set of non-null attribute names are provided, the join results may only contain the specified set of attributes.
   *
   * When two records contain attribute(s) with the same name, the attribute names should be prefixed with the table name.
   * E.g. When joining Employee(SSN, DNO, Name) and Department(DNO, Name), the result attributes are
   *    (SSN, Employee.DNO, Employee.Name, Department.DNO, Department.Name)
   * @param outerIterator the iterator that is at the outer loop of the join
   * @param innerIterator the iterator that is at the inner loop of the join
   * @param predicate the join condition
   * @param attrNames set of attribute names to be projected. If NULL, all attributes will be kept.
   * @return iterator that specify the result records
   */
  Iterator join(Iterator outerIterator, Iterator innerIterator,
                ComparisonPredicate predicate, Set<String> attrNames);


  /**
   * Insert a new record to a table
   * @param tableName the target table name
   * @param record the record to be inserted
   * @param primaryKeys primary keys of the record
   * @return StatusCode
   */
  public StatusCode insert(String tableName, Record record, String[] primaryKeys);

  /**
   * Update records by applying the assignment expression.
   * Records are specified by a given iterator. If the iterator is NULL, all records in the table should be updated.
   * @param tableName the target table name
   * @param assignExp assignment expression to apply
   * @param dataSourceIterator specify the records to update. If NULL, apply the assignment expression to all records in table.
   * @return StatusCode
   */
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator);


  // iterator comes from the select
  // if the iterator is null, delete all records in the table

  /**
   * Delete records from the table.
   * Records are specified by the given iterator. If the iterator is NULL, delete all records in the given table.
   * @param tableName the target table name
   * @param iterator specify the records to delete. If NULL, delete all records in a table.
   * @return StatusCode
   */
  public StatusCode delete(String tableName, Iterator iterator);

  public StatusCode closeDatabase();
}

