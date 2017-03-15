package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;

/**
 * Add connected component ids to vertices in graph.
 */
public class ConnectedComponentIdAdder <T>
    implements GraphAlgorithm<Long, ObjectMap, T, Graph<Long, ObjectMap, T>> {
  private ExecutionEnvironment env;

  public ConnectedComponentIdAdder(ExecutionEnvironment env) {
    this.env = env;
  }

  /**
   * Add connected component ids to vertices based on flink connected components.
   * @param graph input graph
   * @return graph containing vertices with additional property
   * @throws Exception
   */
  @Override
  public Graph<Long, ObjectMap, T> run(Graph<Long, ObjectMap, T> graph) throws Exception {
    Graph<Long, Long, NullValue> workingGraph = prepareForCc(graph, env);

    DataSet<Tuple2<Long, Long>> verticesWithMinIds = workingGraph
        .run(new GSAConnectedComponents<>(Integer.MAX_VALUE))
        .map(vertex -> new Tuple2<>(vertex.getId(), vertex.getValue()))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    return graph.joinWithVertices(verticesWithMinIds, new CcIdVertexJoinFunction());
  }

  /**
   * Replace the vertex values for an existing graph by the vertex id as
   * starting value for connected components computation.
   */
  private static <T> Graph<Long, Long, NullValue> prepareForCc(
      Graph<Long, ObjectMap, T> graph,
      ExecutionEnvironment env) {
    DataSet<Vertex<Long, Long>> vertices = graph.getVertices()
        .map(value -> new Vertex<>(value.getId(), value.getId()))
        .returns(new TypeHint<Vertex<Long, Long>>() {});

    DataSet<Edge<Long, NullValue>> edges = graph.getEdges()
        .map(edge -> new Edge<>(edge.getSource(), edge.getTarget(), NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {});

    return Graph.fromDataSet(vertices, edges, env);
  }
}
