package org.mappinganalysis.graph.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;

/**
 * Implementation for Edge Creator.
 */
public class AllEdgesCreator
      implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {
    private DataSet<Vertex<Long, ObjectMap>> vertices;
    private KeySelector<Vertex<Long, ObjectMap>, Long> keySelector;

  /**
   * Within a set of vertices having connected component ids,
   * compute all edges within each component - only return distinct edges.
   */
  AllEdgesCreator(KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    this.keySelector = keySelector;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public DataSet<Edge<Long, NullValue>> createResult() {
    return vertices.groupBy(keySelector)
        .reduceGroup(new AllEdgesCreateGroupReducer<>());
  }
}
