package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeTriplet;

/**
 * Basic minimal threshold function to reduce merge triplets in merge process.
 *
 * Temporary solution, should be integrated in similarity computation process,
 * not needed for basic edge similarity computation.
 *
 * TODO add T implements 'similarity' or similar
 */
public class MinThresholdFilterFunction<T> implements FilterFunction<T> {
  private final Double threshold;

  public MinThresholdFilterFunction(Double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean filter(T value) throws Exception {
    if (value instanceof MergeGeoTriplet) {
      MergeGeoTriplet triplet = (MergeGeoTriplet) value;
      return triplet.getSimilarity() >= threshold;
    } else  if (value instanceof MergeTriplet) {
      MergeTriplet triplet = (MergeTriplet) value;
      return triplet.getSimilarity() >= threshold;
    } else {
      return false;
    }
  }
}
