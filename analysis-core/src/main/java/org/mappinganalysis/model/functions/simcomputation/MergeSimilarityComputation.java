package org.mappinganalysis.model.functions.simcomputation;

import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;

/**
 * Actual implementation for MergeTriplet
 */
public class MergeSimilarityComputation<T, O> extends SimilarityComputation<T, O> {

  MergeSimilarityComputation(SimilarityFunction<T, O> function,
                             SimilarityStrategy strategy,
                             Double threshold) {
    super(function, strategy, threshold);
  }
}