package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Set;

/**
 * TypeGroupBy helper class, Tuple4 with vertexId, edgeSim, Set of types, compId
 */
public class NeighborTuple extends Tuple4<Long, Double, Set<String>, Long> {
  public NeighborTuple() {
  }

  public NeighborTuple(Long vertexId, Double similarity, Set<String> types, Long compId) {
    super(vertexId, similarity, types, compId);
  }

  public Long getVertexId() {
    return f0;
  }

  public Double getSimilarity() {
    return f1;
  }

  public Set<String> getTypes() {
    return f2;
  }

  public Long getCompId() {
    return f3;
  }
}
