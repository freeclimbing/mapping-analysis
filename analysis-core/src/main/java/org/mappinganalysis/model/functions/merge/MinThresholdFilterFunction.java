package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;

/**
 * Basic minimal threshold function to reduce merge triplets in merge process.
 *
 * Temporary solution, should be integrated in similarity computation process.
 */
public class MinThresholdFilterFunction implements FilterFunction<MergeTriplet> {
    private static final Logger LOG = Logger.getLogger(MinThresholdFilterFunction.class);
  private final Double threshold;

  public MinThresholdFilterFunction(Double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean filter(MergeTriplet value) throws Exception {
//    if (value.getSimilarity() < threshold) {
//      LOG.info("excluded triplet: " + value.toString());
//    }
    return value.getSimilarity() >= threshold;
  }
}
