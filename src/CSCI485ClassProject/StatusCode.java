package CSCI485ClassProject;
/**
 * StatusCode defines the status code that may returns by the {TableManager}
 */
public enum StatusCode {

  SUCCESS,
  INTERNAL_STORAGE_FAILURE,

  TABLE_ALREADY_EXISTS,
  TABLE_CREATION_ATTRIBUTE_INVALID,
  TABLE_CREATION_NO_PRIMARY_KEY,
  TABLE_CREATION_PRIMARY_KEY_NOT_FOUND,
  ATTRIBUTE_TYPE_NOT_SUPPORTED,
  TABLE_NOT_FOUND,
  ATTRIBUTE_ALREADY_EXISTS,
  ATTRIBUTE_NOT_FOUND,

  DATA_RECORD_CREATION_ATTRIBUTES_INVALID,
  DATA_RECORD_PRIMARY_KEYS_UNMATCHED,
  DATA_RECORD_CREATION_ATTRIBUTE_TYPE_UNMATCHED,
  DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS,

  INDEX_ALREADY_EXISTS_ON_ATTRIBUTE,
  INDEX_NOT_FOUND,

  CURSOR_INVALID,
  CURSOR_REACH_TO_EOF,
  CURSOR_NOT_INITIALIZED,

  CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND,

  // ComparisonPredicate
  PREDICATE_OR_EXPRESSION_VALID,
  PREDICATE_OR_EXPRESSION_INVALID,

  // Operator status
  OPERATOR_INSERT_PRIMARY_KEYS_INVALID,
  OPERATOR_INSERT_RECORD_INVALID,

  OPERATOR_UPDATE_ITERATOR_INVALID,
  OPERATOR_UPDATE_ITERATOR_TABLENAME_UNMATCHED,
  OPERATOR_DELETE_ITERATOR_INVALID,
  OPERATOR_DELETE_ITERATOR_TABLENAME_UNMATCHED,

  // Iterator status
  ITERATOR_WRITE_NOT_SUPPORTED,
  ITERATOR_NOT_POINTED_TO_ANY_RECORD


}