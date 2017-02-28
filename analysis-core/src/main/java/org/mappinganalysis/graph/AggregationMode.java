package org.mappinganalysis.graph;

import java.util.HashMap;

/**
 * Aggregate similarity computation results
 */
public abstract class AggregationMode<T> {
  /**
   * Abstract compute method for general aggregation mode
   * @return resulting aggregated similarity value
   */
  public abstract Double compute(HashMap<String, Double> values);
}
