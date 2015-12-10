package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Extract CC Id from Vertex.
 */
public class CcVerticesCreator implements MapFunction<Vertex<Long, ObjectMap>, Long> {
  @Override
  public Long map(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getId();
  }
}
