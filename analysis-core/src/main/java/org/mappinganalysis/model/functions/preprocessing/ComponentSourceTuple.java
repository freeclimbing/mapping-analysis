package org.mappinganalysis.model.functions.preprocessing;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.util.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ComponentSourceTuple extends Tuple2<Long, Integer> {
  private static final Logger LOG = Logger.getLogger(ComponentSourceTuple.class);

  public static final HashMap<String, Integer> CC_SOURCE_MAP;
  static {
    CC_SOURCE_MAP = Maps.newHashMap();
    CC_SOURCE_MAP.put(Constants.DBP_NS, 1);
    CC_SOURCE_MAP.put(Constants.GN_NS, 2);
    CC_SOURCE_MAP.put(Constants.LGD_NS, 4);
    CC_SOURCE_MAP.put(Constants.FB_NS, 8);
    CC_SOURCE_MAP.put(Constants.NYT_NS, 16);
  }

  public ComponentSourceTuple() {
    this.f1 = 0;
  }

  public ComponentSourceTuple(Long ccId) {
    this.f0 = ccId;
    this.f1 = 0;
  }

  public boolean addSource(String source) {
    /**
     * todo config maxSources
     */
    int maxSources = 5;
    int sourcesValue = f1;
    int input = CC_SOURCE_MAP.get(source);
    int startValue = (int) (Math.pow(2, maxSources - 1) + 0.5);
//    LOG.info("###Starting with " + f0 + " input: " + input);
    if (sourcesValue == 0) {
      f1 += input;
      return true;
    }

    for (int i = startValue ; i > 0; i -= i/2) {
//      LOG.info("sv - i: " + sourcesValue + " - " + i + " = " + (sourcesValue-i));
      if (sourcesValue - i >= 0) {
        sourcesValue -= i;
      } else if (input == i && i != 1) {
//        LOG.info("input = " + input + " i: " + i);
        f1 += input;
        return true;
      } else if (sourcesValue == 0) {
        return false;
      }
      if (sourcesValue == 0 && i > input) {
        f1 += input;
//        LOG.info("sourcesValue == 0 && i > input result: " + f1);
        return true;
      }
    }
    LOG.info("should not happen result: " + f1);
    return false;
  }

  public Long getCcId() {
    return f0;
  }
  public void setCcId(Long ccId) {
    f0 = ccId;
  }
  public Set<String> getSources() {
    HashSet<String> result = Sets.newHashSet();
    int sourcesValue = f1;

    if (sourcesValue - 16 >= 0) {
      sourcesValue -= 16;
      result.add(Constants.NYT_NS);
    }
    if (sourcesValue - 8 >= 0) {
      sourcesValue -= 8;
      result.add(Constants.FB_NS);
    }
    if (sourcesValue - 4 >= 0) {
      sourcesValue -= 4;
      result.add(Constants.LGD_NS);
    }
    if (sourcesValue - 2 >= 0) {
      sourcesValue -= 2;
      result.add(Constants.GN_NS);
    }
    if (sourcesValue - 1 >= 0) {
      result.add(Constants.DBP_NS);
    }

    return result;
  }

  public Integer getSourceCount() {
    if (f1 == 1 || f1 == 2 || f1 == 4 || f1 == 8 || f1 == 16) {
      return 1;
    } else if (f1 == 3 || f1 == 5 || f1 == 9 || f1 == 17 || f1 == 6
        || f1 == 10 || f1 == 18 || f1 == 12 || f1 == 20 || f1 == 24) {
      return 2;
    } else if (f1 == 7 || f1 == 11 || f1 == 19 || f1 == 13 || f1 == 21
        || f1 == 25) {
      return 3;
    } else if (f1 == 15 || f1 == 29 || f1 == 27 || f1 == 23) {
      return 4;
    } else if (f1 == 31) {
      return 5;
    } else {
      return 0;
    }
  }
}