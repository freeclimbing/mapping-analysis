package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.functions.EdgeExtractCoGroupFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.LeftSideOnlyJoinFunction;

public class GraphUtils {
  private static final Logger LOG = Logger.getLogger(GraphUtils.class);

  /**
   * Add initial component ids to vertices based on flink connected components.
   * @param graph input graph
   * @return graph containing vertices with additional property
   * @throws Exception
   */
  public static <T> Graph<Long, ObjectMap, T> addCcIdsToGraph(
      Graph<Long, ObjectMap, T> graph, ExecutionEnvironment env) throws Exception {

    Graph<Long, Long, NullValue> workingGraph = prepareForCc(graph, env);

    DataSet<Tuple2<Long, Long>> verticesWithMinIds = workingGraph
        .run(new GSAConnectedComponents<>(5))
        .map(vertex -> new Tuple2<>(vertex.getId(), vertex.getValue()))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    return graph.joinWithVertices(verticesWithMinIds, new CcIdVertexJoinFunction());
  }

  public static Graph<Long, ObjectMap, ObjectMap> applyLinkFilter(Graph<Long, ObjectMap, ObjectMap> graph,
                                                                  ExecutionEnvironment env) throws Exception {
  //TODO
//    DataSet<Tuple3<Long, String, Long>> vertices = graph.getVertices()
//        .map(vertex -> new Tuple3<>(vertex.getId(), vertex.getValue().getOntology(), vertex.getValue().getHashCcId()));
//    DataSet<Tuple3<Long, Long, Double>> edges = graph.getEdges()
//        .map(edge -> new Tuple3<>(edge.getSource(), edge.getTarget(), edge.getValue().getEdgeSimilarity()));
//
//    DeltaIteration<Tuple3<Long, Long, Double>, Tuple3<Long, Long, Double>> iteration = edges
//    .iterateDelta(edges, 1000);
//
//    vertices.groupBy(2);
    return Preprocessing.applyLinkFilterStrategy(graph, env, Boolean.FALSE);
//    return graph;
  }

  /**
   * Replace the vertex values for an existing graph by the vertex id as
   * starting value for connected components computation.
   */
  public static <T> Graph<Long, Long, NullValue> prepareForCc(
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

  /**
   * For a set of vertices, create all single distinct edges which can be
   * created within a connected component and restrict them:
   * - only one edge between 2 vertices
   * - no edge from a -> a
   * @param vertices vertices set
   * @param keySelector selector represents the connected components
   * @return edge set
   */
  public static DataSet<Edge<Long, NullValue>> getTransitiveClosureEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    DataSet<Edge<Long, NullValue>> edgeSet = computeComponentEdges(vertices, keySelector);

    return getDistinctSimpleEdges(edgeSet);
  }

  /**
   * Within a set of vertices, compute all edges for each contained component.
   * @param vertices vertices set
   * @return edge set
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
   * Maps any edge value in a graph to NullValue. JSON imports graphs with edge values,
   * sometimes (e.g., testing) you don't want edge values.
   * @return graph with edge NullValues
   */
  @Deprecated
  public static <EV> Graph<Long, ObjectMap, NullValue> mapEdgesToNullValue(
      Graph<Long, ObjectMap, EV> graph) {
    return graph
        .mapEdges(new MapFunction<Edge<Long, EV>, NullValue>() { // dont replace
          @Override
          public NullValue map(Edge<Long, EV> value) throws Exception {
            return NullValue.getInstance();
          }
        });
  }

  /**
   * Example: (1, 2), (2, 1), (1, 3), (1, 1) as input will result in (1, 2), (1,3)
   */
  public static DataSet<Edge<Long, NullValue>> getDistinctSimpleEdges(
      DataSet<Edge<Long, NullValue>> input) {
    return input
        .filter(edge -> edge.getSource().longValue() != edge.getTarget())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
  }

  /**
   * TODO Uses parts of getDistinctSimpleEdges, rewrite and test
   * only used in cluster computation test
   */
  public static DataSet<Edge<Long, NullValue>> restrictToNewEdges(DataSet<Edge<Long, NullValue>> input,
                                                                  DataSet<Edge<Long, NullValue>> tmpResult) {
    return tmpResult
        .filter(edge -> edge.getSource().longValue() != edge.getTarget())
        .leftOuterJoin(input)
        .where(0, 1)
        .equalTo(0, 1)
        .with(new LeftSideOnlyJoinFunction<>())
        .leftOuterJoin(input)
        .where(0, 1)
        .equalTo(1, 0)
        .with(new LeftSideOnlyJoinFunction<>())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
  }
}
