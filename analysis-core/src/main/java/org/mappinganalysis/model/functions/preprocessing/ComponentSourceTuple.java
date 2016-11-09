package org.mappinganalysis.model.functions.preprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ComponentSourceTuple extends Tuple2<Long, Integer> {
  private static final Logger LOG = Logger.getLogger(ComponentSourceTuple.class);
  private static final HashMap<String, Integer> SOURCES = AbstractionUtils.getSourceMap();
  private static final HashMap<String, Integer> TYPES = AbstractionUtils.getTypesMap();

  public ComponentSourceTuple() {
    this.f1 = 0;
  }

  public ComponentSourceTuple(Long ccId) {
    this.f0 = ccId;
    this.f1 = 0;
  }

  public boolean contains(String source) {
    int maxSources = 5;
    int sourcesValue = f1;
    int input = SOURCES.get(source);
    int startValue = (int) (Math.pow(2, maxSources - 1) + 0.5);
    if (f1 == 0) {
      return false;
    }

    for (int i = startValue ; i > 0; i -= i/2) {
      if (sourcesValue - i >= 0) {
        sourcesValue -= i;
        if (i == input) {
          return true;
        }
      }
      if (i == 1 && sourcesValue == 1) {
        return true;
      }
      if (i == 1 && sourcesValue < 1) {
        return false;
      }
    }

    return false;
  }

  public boolean addSource(String source) {
    /**
     * todo config maxSources
     */
    int maxSources = 5;
    int sourcesValue = f1;
    int input = SOURCES.get(source);
    int startValue = (int) (Math.pow(2, maxSources - 1) + 0.5);
    if (sourcesValue == 0) {
      f1 += input;
      return true;
    }

    for (int i = startValue ; i > 0; i -= i/2) {
      if (sourcesValue - i >= 0) {
        sourcesValue -= i;
      } else if (input == i && i != 1) {
        f1 += input;
        return true;
      } else if (sourcesValue == 0) {
        return false;
      }
      if (sourcesValue == 0 && i > input) {
        f1 += input;
        return true;
      }
    }
    LOG.info("should not happen result: " + f1);
    return false;
  }

  public Long getCcId() {
    return f0;
  }

  public Integer getSourcesInt() {
    return f1;
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
}