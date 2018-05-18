package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FilterFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeMusicTriplet;

/**
 * Basic minimal threshold function to reduce merge triplets in merge process.
 *
 * Temporary solution, should be integrated in similarity computation process,
 * not needed for basic edge similarity computation.
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
    } else  if (value instanceof MergeMusicTriplet) {
      MergeMusicTriplet triplet = (MergeMusicTriplet) value;
      return triplet.getSimilarity() >= threshold;
    } else {
        return false;
    }
  }
}
