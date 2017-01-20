package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

/**
 * For a dataset of vertices, delete vertices which are not source or target of an edge.
 */
public class IsolatedVertexRemover<VV, EV>
    implements CustomUnaryOperation<Vertex<Long, VV>, Vertex<Long, VV>> {

  private final DataSet<Tuple2<Long, Long>> edges;
  private DataSet<Vertex<Long, VV>> initialVertices;

  public IsolatedVertexRemover(DataSet<Edge<Long, EV>> edges) {
    this.edges = edges.<Tuple2<Long, Long>>project(0, 1);
  }

  @Override
  public void setInput(DataSet<Vertex<Long, VV>> inputData) {
      this.initialVertices = inputData;
  }

  /**
   * Check both source and target side of links if a matching vertex is found.
   * @return set of vertices having a match
   */
  @Override
  public DataSet<Vertex<Long, VV>> createResult() {

    DataSet<Vertex<Long, VV>> left = initialVertices
        .join(edges)
        .where(0)
        .equalTo(0)
        .with((vertex, edge) -> vertex)
        .returns(new TypeHint<Vertex<Long, VV>>() {});

    return initialVertices
        .join(edges)
        .where(0)
        .equalTo(1)
        .with((vertex, edge) -> vertex)
        .returns(new TypeHint<Vertex<Long, VV>>() {})
        .union(left)
        .distinct(0);
  }
}
