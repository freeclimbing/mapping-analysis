package org.mappinganalysis.graph;

import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;
import org.mappinganalysis.model.functions.clustering.EdgeExtractCoGroupFunction;
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
        .run(new GSAConnectedComponents<>(1000))
        .map(vertex -> new Tuple2<>(vertex.getId(), vertex.getValue()))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    return graph.joinWithVertices(verticesWithMinIds, new CcIdVertexJoinFunction());
  }

  public static Graph<Long, ObjectMap, ObjectMap> applyLinkFilter(Graph<Long, ObjectMap, ObjectMap> graph,
                                                                  ExecutionEnvironment env) {
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

  private static <T> Graph<Long, Long, NullValue> prepareForCc(Graph<Long, ObjectMap, T> graph, ExecutionEnvironment env) {
    DataSet<Vertex<Long, Long>> vertices = graph.getVertices()
        .map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, Long>>() {
          @Override
          public Vertex<Long, Long> map(Vertex<Long, ObjectMap> value) throws Exception {
            return new Vertex<>(value.getId(), value.getId());
          }
        });

    DataSet<Edge<Long, NullValue>> edges = graph.getEdges()
        .map(edge -> new Edge<>(edge.getSource(), edge.getTarget(), NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {});

    Graph<Long, Long, NullValue> workingGraph = Graph.fromDataSet(vertices, edges, env);

    workingGraph = workingGraph.filterOnVertices(new FilterFunction<Vertex<Long, Long>>() {
      @Override
      public boolean filter(Vertex<Long, Long> value) throws Exception {
        Preconditions.checkArgument(value.getId() != null, "id " + value.toString());
        Preconditions.checkArgument(value.getValue() != null, "value " + value.toString());
        return true;
      }
    });
    return workingGraph;
  }

  /**
   * For a set of vertices, create all single distinct edges which can be
   * created within a connected component.
   * @param vertices vertices set
   * @param keySelector selector represents the connected components
   * @return edge set
   */
  public static DataSet<Edge<Long, NullValue>> getTransitiveClosureEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector) {
    return computeComponentEdges(vertices, keySelector, true);
  }

  /**
   * Within a set of vertices, compute all edges for each contained component,
   * restrict to simple edges with boolean
   * @param vertices vertices set
   * @param keySelector select the cc id selector, most likely hash or cc id
   * @param isSimpleDistinctEdgeSet specify if result set should be restricted (most likely to be true)
   * @return edge set
   */
  private static DataSet<Edge<Long, NullValue>> computeComponentEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      KeySelector<Vertex<Long, ObjectMap>, Long> keySelector,
      boolean isSimpleDistinctEdgeSet) {

    DataSet<Edge<Long, NullValue>> edgeSet = computeComponentEdges(vertices, keySelector);

    if (isSimpleDistinctEdgeSet) {
      return getDistinctSimpleEdges(edgeSet);
    } else {
      return edgeSet;
    }
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
   * Example: (1, 2), (2, 1), (1, 3), (1, 1) as input will result in (1, 2), (1,3)
   */
  public static DataSet<Edge<Long, NullValue>> getDistinctSimpleEdges(DataSet<Edge<Long, NullValue>> input) {
    return input
        .filter(edge -> (long) edge.getSource() != edge.getTarget())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
//        .filter(edge -> {
//            LOG.info("distinctSimpleEdge: " + edge.toString());
//            return true;
//        });
  }

  /**
   * TODO Uses parts of getDistinctSimpleEdges, rewrite and test
   *
   * only used in test
   */
  public static DataSet<Edge<Long, NullValue>> restrictToNewEdges(DataSet<Edge<Long, NullValue>> input,
                                                                  DataSet<Edge<Long, NullValue>> tmpResult) {
    return tmpResult
        .filter(edge -> (long) edge.getSource() != edge.getTarget())
        .leftOuterJoin(input)
        .where(0, 1).equalTo(0, 1)
        .with(new LeftSideOnlyJoinFunction<>())
        .leftOuterJoin(input)
        .where(0, 1).equalTo(1, 0)
        .with(new LeftSideOnlyJoinFunction<>())
        .map(edge -> edge.getSource() < edge.getTarget() ? edge : edge.reverse())
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();
  }
}
