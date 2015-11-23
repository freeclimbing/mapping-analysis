package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.FlinkVertex;

/**
 * Extract CC Id from Vertex.
 */
public class CcVerticesCreator implements MapFunction<Vertex<Long, FlinkVertex>, Long> {
  @Override
  public Long map(Vertex<Long, FlinkVertex> flinkVertex) throws Exception {
    return flinkVertex.getId();
  }
}
