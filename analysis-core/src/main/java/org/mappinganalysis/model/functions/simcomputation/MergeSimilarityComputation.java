package org.mappinganalysis.model.functions.simcomputation;

import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

/**
 * Actual implementation for MergeTriplet
 */
public class MergeSimilarityComputation<T> extends SimilarityComputation<T> {

  MergeSimilarityComputation(SimilarityFunction<T> function, SimilarityStrategy strategy) {
    super(function, strategy);
  }
}
