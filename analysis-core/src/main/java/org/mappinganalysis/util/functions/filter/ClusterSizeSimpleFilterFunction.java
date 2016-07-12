package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Select clusters based on cluster property vertex list size.
 */
public class ClusterSizeSimpleFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  private final int maxClusterSize;

  public ClusterSizeSimpleFilterFunction(int maxClusterSize) {
    this.maxClusterSize = maxClusterSize;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().getVerticesList().size() == maxClusterSize;
  }
}
