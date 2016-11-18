package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;

/**
 * Created by markus on 11/18/16.
 */
public class MinThresholdFilterFunction implements FilterFunction<MergeTriplet> {
    private static final Logger LOG = Logger.getLogger(MinThresholdFilterFunction.class);

  @Override
  public boolean filter(MergeTriplet value) throws Exception {
    if (value.getSimilarity() < 0.8) {
      LOG.info("excluded triplet: " + value.toString());
    }
    return value.getSimilarity() >= 0.8;
  }
}
