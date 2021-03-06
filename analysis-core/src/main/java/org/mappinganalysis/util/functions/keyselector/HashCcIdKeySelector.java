package org.mappinganalysis.util.functions.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Key Selector hash connected component id
 */
public class HashCcIdKeySelector implements KeySelector<Vertex<Long, ObjectMap>, Long> {
  @Override
  public Long getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().getHashCcId();
  }
}
