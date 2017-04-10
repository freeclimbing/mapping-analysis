package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.functions.EdgeExtractCoGroupFunction;
import org.mappinganalysis.model.ObjectMap;

/**
 * Implementation for Edge Creator.
 */
public class AllEdgesCreator
      implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {
    private DataSet<Vertex<Long, ObjectMap>> vertices;
    private KeySelector<Vertex<Long, ObjectMap>, Long> keySelector;
  private Boolean isResultEdgeDistinct;

  /**
   * Within a set of vertices having connected component ids,
   * compute all edges within each component - optionally return also duplicate edges.
   */
  public AllEdgesCreator(KeySelector<Vertex<Long, ObjectMap>, Long> keySelector, Boolean isResultEdgeDistinct) {
    this.keySelector = keySelector;
    this.isResultEdgeDistinct = isResultEdgeDistinct;
  }

  /**
   * Within a set of vertices having connected component ids,
   * compute all edges within each component - only return distinct edges.
   */
  public AllEdgesCreator(KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    this.keySelector = keySelector;
    this.isResultEdgeDistinct = true;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public DataSet<Edge<Long, NullValue>> createResult() {
    DataSet<Edge<Long, NullValue>> edges = vertices.coGroup(vertices)
        .where(keySelector)
        .equalTo(keySelector)
        .with(new EdgeExtractCoGroupFunction());

    // Example: (1, 2), (2, 1), (1, 3), (1, 1) as input will result in (1, 2), (1,3)
    if (isResultEdgeDistinct) {
      edges = edges
        .filter(edge -> edge.getSource().longValue() != edge.getTarget())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
    }

    return edges;
  }
}
