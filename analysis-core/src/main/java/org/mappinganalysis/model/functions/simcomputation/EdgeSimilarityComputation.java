package org.mappinganalysis.model.functions.simcomputation;

import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;

/**
 * Actual implementation for computing similarities for all edges in a given graph.
 */
public class EdgeSimilarityComputation<T, O> extends SimilarityComputation<T, O> {

  EdgeSimilarityComputation(SimilarityFunction<T, O> function,
                            SimilarityStrategy strategy,
                            Double threshold) {
    super(function, strategy, threshold);
  }
}