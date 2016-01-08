package org.mappinganalysis.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.functions.ExcludeOneToManyOntologiesFilter;
import org.mappinganalysis.model.functions.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.NeighborOntologyFunction;

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
      LOG.info("Applying basic link filter strategy...");

      DataSet<Edge<Long, NullValue>> edgesNoDuplicates = graph
          .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.OUT)
          .groupBy(1, 2)
          .aggregate(Aggregations.SUM, 3)
          .filter(new ExcludeOneToManyOntologiesFilter())
          .map(new MapFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>,
              Edge<Long, NullValue>>() {
            @Override
            public Edge<Long, NullValue> map(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple)
                throws Exception {
              return tuple.f0;
            }
          });

      LOG.info("Done.");

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
  public static Graph<Long, ObjectMap, NullValue> applyTypePreprocessing(
      Graph<Long, ObjectMap, NullValue> graph, ExecutionEnvironment env) {
    LOG.info("Applying type preprocessing...");

    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .getVertices()
        .map(new InternalTypeMapFunction());

    LOG.info("Done.");
    return Graph.fromDataSet(vertices, graph.getEdges(), env);
  }

}
