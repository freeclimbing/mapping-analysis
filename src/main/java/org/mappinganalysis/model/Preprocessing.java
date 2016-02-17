package org.mappinganalysis.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.preprocessing.*;

/**
 * Preprocessing.
 */
public class Preprocessing {
  private static final Logger LOG = Logger.getLogger(Preprocessing.class);

  /**
   * Preprocessing strategy to restrict resources to have only one counterpart in every target ontology.
   *
   * First strategy: delete all links which are involved in 1:n mappings
   * @param graph input graph
   * @param env environment
   * @param isLinkFilterActive boolean if filter should be used
   * @return output graph
   */
  public static Graph<Long, ObjectMap, NullValue> applyLinkFilterStrategy(
      Graph<Long, ObjectMap, NullValue> graph, ExecutionEnvironment env,
      boolean isLinkFilterActive) {
    if (isLinkFilterActive) {
      LOG.info("[1] Preprocessing: Apply basic link filter strategy...");
      DataSet<Edge<Long, NullValue>> edgesNoDuplicates = graph
          .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.OUT)
          .groupBy(1, 2)
          .aggregate(Aggregations.SUM, 3)
          .filter(new ExcludeOneToManyOntologiesFilter()) // deleted links accumulator
          .map(new MapFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>,
              Edge<Long, NullValue>>() {
            @Override
            public Edge<Long, NullValue> map(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple)
                throws Exception {
              return tuple.f0;
            }
          });

      return Graph.fromDataSet(graph.getVertices(), edgesNoDuplicates, env);
    } else {
      return graph;
    }
  }

  /**
   * Harmonize available type information with a common dictionary.
   * @param graph input graph
   * @param env environment
   * @return graph with additional internal type property
   */
  public static Graph<Long, ObjectMap, NullValue> applyTypeToInternalTypeMapping(
      Graph<Long, ObjectMap, NullValue> graph, ExecutionEnvironment env) {
    LOG.info("Apply type preprocessing...");
    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .getVertices()
        .map(new InternalTypeMapFunction());

    return Graph.fromDataSet(vertices, graph.getEdges(), env);
  }

  public static Graph<Long, ObjectMap, NullValue> applyTypeMissMatchCorrection(Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    DataSet<Tuple2<Long, String>> vertexIdAndTypeList = graph.getVertices()
        .map(new VertexIdTypeTupleMapper());

    DataSet<Edge<Long, NullValue>> edgesEqualType = graph.getEdges()
        .map(new MapFunction<Edge<Long, NullValue>, Tuple4<Long, Long, String, String>>() {
          @Override
          public Tuple4<Long, Long, String, String> map(Edge<Long, NullValue> edge) throws Exception {
            return new Tuple4<>(edge.getSource(), edge.getTarget(), "", "");
          }
        })
        .join(vertexIdAndTypeList)
        .where(0).equalTo(0)
        .with(new EdgeTypeJoinFunction(0))
        .join(vertexIdAndTypeList).where(1).equalTo(0)
        .with(new EdgeTypeJoinFunction(1))
        .filter(new FilterNotEqualTypeEdges())
        .map(new MapFunction<Tuple4<Long, Long, String, String>, Edge<Long, NullValue>>() {
          @Override
          public Edge<Long, NullValue> map(Tuple4<Long, Long, String, String> tuple) throws Exception {
            return new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance());
          }
        });

    if (edgesEqualType.collect().isEmpty()) {
      LOG.info("No edge with equal type on source and target.");
    } else {
      graph = graph.removeEdges(edgesEqualType.collect());
      LOG.info(edgesEqualType.count() + " edges with equal type on source and target deleted.");
    }
    return graph;
  }

}
