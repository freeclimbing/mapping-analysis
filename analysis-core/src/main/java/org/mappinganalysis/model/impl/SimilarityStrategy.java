package org.mappinganalysis.model.impl;

/**
 * Define similarity computation strategy.
 */
public enum SimilarityStrategy {
  /**
   * {@see MergeSimilarityComputation}
   */
  MERGE,
  EDGE_SIM,
  MUSIC,
  NC
}
