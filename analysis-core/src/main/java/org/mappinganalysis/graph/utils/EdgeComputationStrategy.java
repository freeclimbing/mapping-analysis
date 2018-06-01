package org.mappinganalysis.graph.utils;

/**
 * Edge computation strategies
 */
public enum EdgeComputationStrategy {
  /**
   * {@see EdgeComputationVertexCcSet}
   */
  /*
   The minimum amount of links based on KeySelector to connect all affected elements.
   */
  SIMPLE,
  /*
   all non-duplicate links
   */
  ALL,
  /*
  create gold links from representatives
   */
  REPRESENTATIVE,
  NONE
}
