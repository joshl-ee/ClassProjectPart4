package CSCI485ClassProject.utils;

import CSCI485ClassProject.models.AlgebraicOperator;
import CSCI485ClassProject.models.ComparisonOperator;

public class ComparisonUtils {
  public static boolean compareTwoINT(Object obj1, Object obj2, ComparisonOperator cmp) {
    long val1;
    if (obj1 instanceof Integer) {
      val1 = new Long((Integer) obj1);
    } else {
      val1 = (long) obj1;
    }

    long val2;
    if (obj2 instanceof Integer) {
      val2 = new Long((Integer) obj2);
    } else {
      val2 = (long) obj2;
    }

    if (cmp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      // >=
      return val1 >= val2;

    } else if (cmp == ComparisonOperator.GREATER_THAN) {
      // >
      return val1 > val2;
    } else if (cmp == ComparisonOperator.EQUAL_TO) {
      // ==
      return val1 == val2;
    } else if (cmp == ComparisonOperator.LESS_THAN) {
      // <
      return val1 < val2;
    } else {
      // <=
      return val1 <= val2;
    }
  }

  public static boolean compareTwoDOUBLE(Object obj1, Object obj2, ComparisonOperator cmp) {
    double val1 = (double) obj1;
    double val2 = (double) obj2;

    if (cmp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      // >=
      return val1 >= val2;

    } else if (cmp == ComparisonOperator.GREATER_THAN) {
      // >
      return val1 > val2;
    } else if (cmp == ComparisonOperator.EQUAL_TO) {
      // ==
      return val1 == val2;
    } else if (cmp == ComparisonOperator.LESS_THAN) {
      // <
      return val1 < val2;
    } else {
      // <=
      return val1 <= val2;
    }
  }

  public static boolean compareTwoVARCHAR(Object obj1, Object obj2, ComparisonOperator cmp) {
    String val1 = (String) obj1;
    String val2 = (String) obj2;

    if (cmp == ComparisonOperator.GREATER_THAN_OR_EQUAL_TO) {
      // >=
      return val1.equals(val2) || val1.compareTo(val2) > 0;

    } else if (cmp == ComparisonOperator.GREATER_THAN) {
      // >
      return val1.compareTo(val2) > 0;
    } else if (cmp == ComparisonOperator.EQUAL_TO) {
      // ==
      return val1.equals(val2);
    } else if (cmp == ComparisonOperator.LESS_THAN) {
      // <
      return val1.compareTo(val2) < 0;
    } else {
      // <=
      return val1.compareTo(val2) < 0 || val1.equals(val2);
    }
  }

  // Calculate method for int
  public static long calculateINT(Object attrValue, AlgebraicOperator operator, Object rhsValue) {
    long val1;
    if (attrValue instanceof Integer) {
      val1 = new Long((Integer) attrValue);
    } else {
      val1 = (long) attrValue;
    }

    long val2;
    if (rhsValue instanceof Integer) {
      val2 = new Long((Integer) rhsValue);
    } else {
      val2 = (long) rhsValue;
    }

    if (operator == AlgebraicOperator.PRODUCT) {
      return val1 * val2;
    }
    else if (operator == AlgebraicOperator.DIVISION) {
      return val1/val2;
    }
    else if (operator == AlgebraicOperator.MINUS) {
      return val1-val2;
    }
    else if (operator == AlgebraicOperator.PLUS) {
      return val1+val2;
    }
    return -1;
  }

  // Calculate method for DOUBLE. NOT TESTED
  public static double calculateDOUBLE(Object attrValue, AlgebraicOperator operator, Object rhsValue) {
    double val1 = (double) attrValue;
    double val2 = (double) rhsValue;

    if (operator == AlgebraicOperator.PRODUCT) {
      return val1 * val2;
    }
    else if (operator == AlgebraicOperator.DIVISION) {
      return val1/val2;
    }
    else if (operator == AlgebraicOperator.MINUS) {
      return val1-val2;
    }
    else if (operator == AlgebraicOperator.PLUS) {
      return val1+val2;
    }
    return -1;
  }
}
