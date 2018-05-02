package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.config.Config;

class BlockingKeyMapFunction implements MapFunction<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>> {
  private Config config;

  BlockingKeyMapFunction(Config config) {
    this.config = config;
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    vertex.getValue().setMode(config.getDataDomain());
    vertex.getValue().setBlockingKey(config.getBlockingStrategy());

    return vertex;
  }
}
