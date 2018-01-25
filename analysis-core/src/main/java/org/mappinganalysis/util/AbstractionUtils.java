package org.mappinganalysis.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.preprocessing.utils.ComponentSourceTuple;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Data source related helper classes. Currently, only 5 sources are supported:
 * DBpedia, GeoNames, LinkedGeoData, Freebase, Nyt
 */
public class AbstractionUtils {
  private static final Logger LOG = Logger.getLogger(AbstractionUtils.class);

  private static HashMap<String, Integer> SOURCES_MAP;

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

  /**
   * For a list of data sources, create a HashMap with corresponding integer values.
   */
  public static HashMap<String, Integer> getSourcesMap(List<String> sources) {
    SOURCES_MAP = Maps.newHashMap();
    Collections.sort(sources);
    int running = 1;

//    System.out.println("AbstractionUtils: " + sources);
    for (String source : sources) {
      SOURCES_MAP.put(source, running);
      running *= 2;
    }

    return SOURCES_MAP;
  }

  /**
   * Return an integer representation of the used data sources for easy use in tuples.
   */
  public static int getSourcesInt(String mode, Set<String> sources) {
    setupMode(mode);
    int result = 0;
    for (String source : sources) {
      result += SOURCES_MAP.get(source);
    }

    return result;
  }

  private static void setupMode(String mode) {
    if (mode.equals(Constants.MUSIC)) {
      SOURCES_MAP = Constants.MUSIC_MAP;
    } else if (mode.equals(Constants.GEO)) {
      SOURCES_MAP = Constants.GEO_MAP;
    } else if (mode.equals(Constants.NC)) {
      SOURCES_MAP = Constants.NC_MAP;
    }
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

    int maxSourceValue = 512;
    while (maxSourceValue > 0) {
      if (value - maxSourceValue >= 0) {
        value -= maxSourceValue;
        result.add(maxSourceValue);
      }
      maxSourceValue = maxSourceValue >> 1;
    }

    return result;
  }

  public static Set<String> getSourcesStringSet(String mode, Integer value) {
    setupMode(mode);
    Set<Integer> valuesIntSet = getValuesIntSet(value);
    Set<String> result = Sets.newHashSet();

    result.addAll(SOURCES_MAP
        .entrySet()
        .stream()
        .filter(entry -> valuesIntSet.contains(entry.getValue()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList()));

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

  public static int getSourceCount(ComponentSourceTuple tuple) {
    return getSourceCount(tuple.getSourcesInt());
  }

  /**
   * Given the int abstraction of a property, resolve the number of contained elements.
   */
  public static int getSourceCount(Integer srcInt) {
    Set<Integer> valuesIntSet = getValuesIntSet(srcInt);

    return valuesIntSet.size();
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
  public static boolean containsSrc(String mode, Integer sources, String checkSrc) {
    setupMode(mode);
    Set<Integer> values = getValuesIntSet(sources);
    Integer checkInt = SOURCES_MAP.get(checkSrc);

    return checkInt != null && values.contains(checkInt);
  }
}
