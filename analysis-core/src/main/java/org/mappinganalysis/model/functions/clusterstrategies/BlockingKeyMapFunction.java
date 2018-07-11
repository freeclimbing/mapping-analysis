package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Add data domain and blocking key for a given vertex.
 * Needed sometimes after reading vertices from file.
 */
class BlockingKeyMapFunction
    implements MapFunction<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>> {
  private IncrementalConfig config;

  BlockingKeyMapFunction(IncrementalConfig config) {
    this.config = config;
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    vertex.getValue().setMode(config.getDataDomain());
    vertex.getValue().setBlockingKey(
        config.getBlockingStrategy(),
        config.getBlockingLength());
    return vertex;
  }
}
