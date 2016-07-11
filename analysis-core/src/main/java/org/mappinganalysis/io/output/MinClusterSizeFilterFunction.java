package org.mappinganalysis.io.output;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

class MinClusterSizeFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  private final int minSize;

  public MinClusterSizeFilterFunction(int minSize) {
    this.minSize = minSize;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().getVerticesList().size() >= minSize;
  }
}
