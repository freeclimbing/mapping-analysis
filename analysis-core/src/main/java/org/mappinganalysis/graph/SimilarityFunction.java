package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Abstract class for all similarity functions.
 */
public abstract class SimilarityFunction<T, O>
    implements MapFunction<T, O> {
  public String metric;
}
