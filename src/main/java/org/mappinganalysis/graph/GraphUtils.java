package org.mappinganalysis.graph;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;
import org.mappinganalysis.model.functions.VertexIdMapFunction;
import org.mappinganalysis.utils.functions.keyselector.CcIdKeySelector;
import org.mappinganalysis.model.functions.clustering.EdgeExtractCoGroupFunction;
import org.mappinganalysis.utils.functions.LeftSideOnlyJoinFunction;
import org.mappinganalysis.utils.Utils;

public class GraphUtils {

  /**
   * Add initial component ids to vertices based on flink connected components.
   * @param graph input graph
   * @return graph containing vertices with additional property
   * @throws Exception
   */
  public static <T> Graph<Long, ObjectMap, T> addCcIdsToGraph(
      Graph<Long, ObjectMap, T> graph) throws Exception {

    final DataSet<Tuple2<Long, Long>> components = FlinkConnectedComponents
        .compute(graph.getVertices().map(new VertexIdMapFunction()),
            graph.getEdgeIds(),
            1000);

    return graph.joinWithVertices(components, new CcIdVertexJoinFunction());
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
    if (isSimpleDistinctEdgeSet) {
      DataSet<Edge<Long, NullValue>> edgeSet = computeComponentEdges(vertices, keySelector);
      return getDistinctSimpleEdges(edgeSet);
    } else {
      return computeComponentEdges(vertices, keySelector);
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
        .distinct()
        .filter(new RichFilterFunction<Edge<Long, NullValue>>() {
          private LongCounter restrictEdgeCounter = new LongCounter();

          @Override
          public void open(final Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(Utils.RESTRICT_EDGE_COUNT_ACCUMULATOR, restrictEdgeCounter);
          }
          @Override
          public boolean filter(Edge<Long, NullValue> longNullValueEdge) throws Exception {
            restrictEdgeCounter.add(1L);
            return true;
          }
        });
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
