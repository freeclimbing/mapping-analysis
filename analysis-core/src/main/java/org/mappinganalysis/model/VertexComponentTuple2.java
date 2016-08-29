package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple2;

public class VertexComponentTuple2 extends Tuple2<Long, Long> {
  public VertexComponentTuple2() {
  }

  public VertexComponentTuple2(Long vertexId, Long componentId) {
    super(vertexId, componentId);
  }

  public Long getVertexId() {
    return f0;
  }

  public Long getComponentId() {
    return f1;
  }
}
