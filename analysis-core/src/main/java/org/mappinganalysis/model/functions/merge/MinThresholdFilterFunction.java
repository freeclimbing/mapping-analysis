package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;

/**
 * Basic minimal threshold function to reduce merge triplets in merge process.
 *
 * Temporary solution, should be integrated in similarity computation process,
 * not needed for basic edge similarity computation.
 */
public class MinThresholdFilterFunction<T> implements FilterFunction<T> {
//    private static final Logger LOG = Logger.getLogger(MinThresholdFilterFunction.class);
  private final Double threshold;

  public MinThresholdFilterFunction(Double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean filter(T value) throws Exception {
//    if (value.getSimilarity() < threshold) {
//      LOG.info("excluded triplet: " + value.toString());
//    }

    if (value instanceof MergeTriplet) {
      MergeTriplet triplet = (MergeTriplet) value;
      return triplet.getSimilarity() >= threshold;
    } else {
      return false;
    }
  }
}
