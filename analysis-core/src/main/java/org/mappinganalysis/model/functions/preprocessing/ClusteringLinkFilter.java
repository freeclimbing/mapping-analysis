package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * actual implementation for initial clustering use case
 */
public class ClusteringLinkFilter extends LinkFilter {
  public ClusteringLinkFilter(ExecutionEnvironment env) {
    super(new ClusteringLinkFilterFunction(env));
  }
}
