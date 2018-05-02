package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

public class SimSortInputGraphCreator
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, SimSortVertexTuple, SimSortEdgeTuple>> {
  private static final Logger LOG = Logger.getLogger(SimSortInputGraphCreator.class);
  private ExecutionEnvironment env;

  /**
   * Converts input graph to SimSort input format using TupleX instead of complex ObjectMap.
   */
  SimSortInputGraphCreator(ExecutionEnvironment env) {
    this.env = env;
  }

  @Override
  public Graph<Long, SimSortVertexTuple, SimSortEdgeTuple> run(
      Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
    DataSet<Edge<Long, SimSortEdgeTuple>> edges = graph.getEdges()
        .map(edge -> new Edge<>(edge.getSource(),
            edge.getTarget(),
            new SimSortEdgeTuple(edge.getValue().getEdgeSimilarity())))
        .returns(new TypeHint<Edge<Long, SimSortEdgeTuple>>() {});

    DataSet<Vertex<Long, SimSortVertexTuple>> vertices = graph
        .getVertices()
        .map(vertex -> new Vertex<>(vertex.getId(),
            new SimSortVertexTuple(vertex.getValue().getHashCcId(),
                Long.MIN_VALUE, // not safe to assume
                -1D,
                Boolean.TRUE)))
        .returns(new TypeHint<Vertex<Long, SimSortVertexTuple>>() {});

    return Graph.fromDataSet(vertices, edges, env);
  }
}
