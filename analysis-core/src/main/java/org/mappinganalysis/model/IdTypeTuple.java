package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple2;

public class IdTypeTuple extends Tuple2<Long, String> {
  public IdTypeTuple() {
  }

  public IdTypeTuple(Long vertexId, String type) {
    super(vertexId, type);
  }

  public Long getVertexId() {
    return f0;
  }

  public String getType() {
    return f1;
  }
}
