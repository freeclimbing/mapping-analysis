package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

public class ClusterSizeFilterFunction extends RichFilterFunction<Vertex<Long, ObjectMap>> {
  private Integer maxClusterSize = null;
  private int superstep;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.superstep = getIterationRuntimeContext().getSuperstepNumber();
  }

  /**
   * Get all clusters up to size minus maximum cluster size.
   */
  public ClusterSizeFilterFunction(int maxClusterSize) {
    this.maxClusterSize = maxClusterSize;
  }

  public ClusterSizeFilterFunction() {
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (maxClusterSize != null) {
      return vertex.getValue().getVerticesList().size() <= maxClusterSize - superstep;
    } else {
      return vertex.getValue().getVerticesList().size() == superstep;
    }
  }
}
