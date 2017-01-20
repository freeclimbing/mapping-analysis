package org.mappinganalysis.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.mappinganalysis.model.functions.preprocessing.utils.ComponentSourceTuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data source related helper classes. Currently, only 5 sources are supported:
 * DBpedia, GeoNames, LinkedGeoData, Freebase, Nyt
 *
 * TODO added type equivalent, needs testing
 */
public class AbstractionUtils {
  private static final HashMap<String, Integer> SOURCES_MAP;
  static {
    SOURCES_MAP = Maps.newHashMap();
    SOURCES_MAP.put(Constants.DBP_NS, 1);
    SOURCES_MAP.put(Constants.GN_NS, 2);
    SOURCES_MAP.put(Constants.LGD_NS, 4);
    SOURCES_MAP.put(Constants.FB_NS, 8);
    SOURCES_MAP.put(Constants.NYT_NS, 16);
  }

  // todo check no type compatibility
  private static final HashMap<String, Integer> TYPES_MAP;
  static {
    TYPES_MAP = Maps.newHashMap();
    TYPES_MAP.put(Constants.NO_TYPE, 0);
    TYPES_MAP.put(Constants.P, 1);
    TYPES_MAP.put(Constants.B, 2);
    TYPES_MAP.put(Constants.AR, 4);
    TYPES_MAP.put(Constants.M, 8);
    TYPES_MAP.put(Constants.AS, 16);
  }

  public static HashMap<String, Integer> getSourceMap() {
    return SOURCES_MAP;
  }

  public static HashMap<String, Integer> getTypesMap() {
    return TYPES_MAP;
  }

//  public boolean contains(String source) {
//    int maxSources = 5; // todo check
//    int sourcesValue = f1;
//    int input = SOURCES.get(source);
//    int startValue = (int) (Math.pow(2, maxSources - 1) + 0.5);
//    if (sourcesValue == 0) {
//      return false;
//    }
//
//    for (int i = startValue ; i > 0; i -= i/2) {
//      if (sourcesValue - i >= 0) {
//        sourcesValue -= i;
//        if (i == input) {
//          return true;
//        }
//      }
//      if (i == 1 && sourcesValue == 1) {
//        return true;
//      }
//      if (i == 1 && sourcesValue < 1) {
//        return false;
//      }
//    }
//
//    return false;
//  }

  /**
   * Return an integer representation of the used data sources for easy use in tuples.
   */
  public static Integer getSourcesInt(Set<String> sources) {
    int result = 0;
    for (String source : sources) {
      result += SOURCES_MAP.get(source);
    }

    return result;
  }

  /**
   * Return an integer representation of the type property.
   */
  public static Integer getTypesInt(Set<String> types) {
    int result = 0;
    for (String type : types) {
      result += TYPES_MAP.get(type);
    }

    return result;
  }

  /**
   * Merge two int representations of values into a single one.
   */
  public static Integer mergeIntValues(Integer left, Integer right) {
    Set<Integer> valuesIntSet = getValuesIntSet(left);
    valuesIntSet.addAll(getValuesIntSet(right));

    return valuesIntSet.stream().mapToInt(i -> i).sum();
  }

  private static Set<Integer> getValuesIntSet(Integer value) {
    HashSet<Integer> result = Sets.newHashSet();

    if (value - 16 >= 0) {
      value -= 16;
      result.add(16);
    }
    if (value - 8 >= 0) {
      value -= 8;
      result.add(8);
    }
    if (value - 4 >= 0) {
      value -= 4;
      result.add(4);
    }
    if (value - 2 >= 0) {
      value -= 2;
      result.add(2);
    }
    if (value - 1 >= 0) {
      result.add(1);
    }

    return result;
  }

  public static Set<String> getSourcesStringSet(Integer value) {
    Set<Integer> valuesIntSet = getValuesIntSet(value);
    Set<String> result = Sets.newHashSet();
    for (Map.Entry<String, Integer> entry : SOURCES_MAP.entrySet()) {
      if (valuesIntSet.contains(entry.getValue())) {
        result.add(entry.getKey());
      }
    }

    return result;
  }

  public static Set<String> getTypesStringSet(Integer value) {
    Set<Integer> valuesIntSet = getValuesIntSet(value);
    Set<String> result = Sets.newHashSet();
    for (Map.Entry<String, Integer> entry : TYPES_MAP.entrySet()) {
      if (valuesIntSet.contains(entry.getValue())) {
        result.add(entry.getKey());
      }
    }

    return result;
  }

  public static Integer getSourceCount(ComponentSourceTuple tuple) {
    return getSourceCount(tuple.getSourcesInt());
  }

  /**
   * Given the int abstraction of a property, resolve the number of contained elements.
   */
  public static Integer getSourceCount(Integer srcInt) {
    if (srcInt == 1 || srcInt == 2 || srcInt == 4 || srcInt == 8 || srcInt == 16) {
      return 1;
    } else if (srcInt == 3 || srcInt == 5 || srcInt == 9 || srcInt == 17 || srcInt == 6
        || srcInt == 10 || srcInt == 18 || srcInt == 12 || srcInt == 20 || srcInt == 24) {
      return 2;
    } else if (srcInt == 7 || srcInt == 11 || srcInt == 19 || srcInt == 13 || srcInt == 21
        || srcInt == 25) {
      return 3;
    } else if (srcInt == 15 || srcInt == 29 || srcInt == 27 || srcInt == 23) {
      return 4;
    } else if (srcInt == 31) {
      return 5;
    } else {
      return 0;
    }
  }

  /**
   * Check if overlap in sources or types can be found.
   * For types, the special case "0" equals "no type" corresponding to an overlap.
   */
  public static boolean hasOverlap(Integer left, Integer right) {
    if (left == 0 || right == 0) {
      return true;
    }
    Set<Integer> rightSide = getValuesIntSet(right);
    for (Integer leftValue : getValuesIntSet(left)) {
      if (rightSide.contains(leftValue)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Check if certain int sources representation contains a specific source value.
   */
  public static boolean containsSrc(Integer sources, String checkSrc) {
    Set<Integer> values = getValuesIntSet(sources);

    Integer checkInt = SOURCES_MAP.get(checkSrc);

    return checkInt != null && values.contains(checkInt);
  }
}
