package org.mappinganalysis.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.mappinganalysis.model.functions.preprocessing.ComponentSourceTuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class SourcesUtils {
  private static final HashMap<String, Integer> SOURCES_MAP;
  static {
    SOURCES_MAP = Maps.newHashMap();
    SOURCES_MAP.put(Constants.DBP_NS, 1);
    SOURCES_MAP.put(Constants.GN_NS, 2);
    SOURCES_MAP.put(Constants.LGD_NS, 4);
    SOURCES_MAP.put(Constants.FB_NS, 8);
    SOURCES_MAP.put(Constants.NYT_NS, 16);
  }

  public static HashMap<String, Integer> getSourceMap() {
    return SOURCES_MAP;
  }

  public static Integer getSourcesInt(Set<String> sources) {
    int result = 0;
    for (String source : sources) {
      result += SOURCES_MAP.get(source);
    }

    return result;
  }

  private static Set<Integer> getSourcesIntSet(Integer sourcesValue) {
    HashSet<Integer> result = Sets.newHashSet();

    if (sourcesValue - 16 >= 0) {
      sourcesValue -= 16;
      result.add(16);
    }
    if (sourcesValue - 8 >= 0) {
      sourcesValue -= 8;
      result.add(8);
    }
    if (sourcesValue - 4 >= 0) {
      sourcesValue -= 4;
      result.add(4);
    }
    if (sourcesValue - 2 >= 0) {
      sourcesValue -= 2;
      result.add(2);
    }
    if (sourcesValue - 1 >= 0) {
      result.add(1);
    }

    return result;
  }

  public static Integer getSourceCount(ComponentSourceTuple tuple) {
    return getSourceCount(tuple.getSourcesInt());
  }

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

  public static boolean hasOverlap(Integer left, Integer right) {
    Set<Integer> rightSide = getSourcesIntSet(right);
    for (Integer leftValue : getSourcesIntSet(left)) {
      if (rightSide.contains(leftValue)) {
        return true;
      }
    }

    return false;
  }
}
