package org.mappinganalysis.model.functions.incremental;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

public class BlockingKeySelector
    implements KeySelector<Vertex<Long,ObjectMap>, String> {
  @Override
  public String getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().getBlockingKey();
  }
}
