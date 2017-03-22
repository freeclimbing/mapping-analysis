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
 * Create edges for a given set of vertices having component ids.
 */
public class EdgeComputationVertexCcSet
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Edge<Long, NullValue>> {

  KeySelector<Vertex<Long, ObjectMap>, Long> keySelector;
  private DataSet<Vertex<Long, ObjectMap>> vertices;

  public EdgeComputationVertexCcSet(KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    this.keySelector = keySelector;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  /**
   * For a set of vertices, create all single distinct edges which can be
   * created within a connected component and restrict them:
   * - only one edge between 2 vertices
   * - no edge from a -> a
   * @return edge set
   */
  @Override
  public DataSet<Edge<Long, NullValue>> createResult() {
    DataSet<Edge<Long, NullValue>> edgeSet = computeComponentEdges(vertices, keySelector);

    return getDistinctSimpleEdges(edgeSet);
  }

  /**
   * Within a set of vertices having connected component ids,
   * compute all edges within each component.
   * TODO fix public
   */
  public static DataSet<Edge<Long, NullValue>> computeComponentEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    return vertices.coGroup(vertices)
        .where(keySelector)
        .equalTo(keySelector)
        .with(new EdgeExtractCoGroupFunction());
  }

  /**
   * Example: (1, 2), (2, 1), (1, 3), (1, 1) as input will result in (1, 2), (1,3)
   * TODO fix public
   */
  public static DataSet<Edge<Long, NullValue>> getDistinctSimpleEdges(
      DataSet<Edge<Long, NullValue>> input) {
    return input
        .filter(edge -> edge.getSource().longValue() != edge.getTarget())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
  }
}
