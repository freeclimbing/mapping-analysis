package org.mappinganalysis.model.functions.simcomputation;

import org.mappinganalysis.graph.SimilarityFunction;

/**
 * Actual implementation for computing similarities for all edges in a given graph.
 */
public class EdgeSimilarityComputation<T, O> extends SimilarityComputation<T, O> {

  EdgeSimilarityComputation(SimilarityFunction<T, O> function,
                            Double threshold) {
    super(function, threshold);
  }
}