package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;

/**
 * Delete edges where source or target vertex are not in the vertex set.
 * TODO fix lambdas not working because of type erasure, even with TypeHint
 */
public class IsolatedEdgeRemover<VV, EV>
    implements CustomUnaryOperation<Edge<Long, EV>, Edge<Long, EV>> {

  private final DataSet<Vertex<Long, VV>> vertices;
  private DataSet<Edge<Long, EV>> initialEdges;

  public IsolatedEdgeRemover(DataSet<Vertex<Long, VV>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public void setInput(DataSet<Edge<Long, EV>> inputData) {
    this.initialEdges = inputData;
  }

  /**
   * Check both source and target side of links if a matching vertex is found.
   * @return set of vertices having a match
   */
  @Override
  public DataSet<Edge<Long, EV>> createResult() {
    return initialEdges.join(vertices)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<Edge<Long,EV>, Vertex<Long,VV>, Edge<Long, EV>>() {
          @Override
          public Edge<Long, EV> join(Edge<Long, EV> first, Vertex<Long, VV> second) throws Exception {
            return first;
          }
        })
        .join(vertices)
        .where(1)
        .equalTo(0)
        .with(new JoinFunction<Edge<Long, EV>, Vertex<Long, VV>, Edge<Long, EV>>() {
          @Override
          public Edge<Long, EV> join(Edge<Long, EV> first, Vertex<Long, VV> second) throws Exception {
            return first;
          }
        })
        .distinct(0,1);
  }
}
