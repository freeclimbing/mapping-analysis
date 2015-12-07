package org.mappinganalysis.model;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.functions.ExcludeOneToManyOntologiesFilter;
import org.mappinganalysis.model.functions.NeighborOntologyFunction;
import org.mappinganalysis.utils.TypeDictionary;
import org.mappinganalysis.utils.Utils;

import java.util.Map;
import java.util.Set;

/**
 * Preprocessing.
 */
public class Preprocessing {

  /**
   * Preprocessing strategy to restrict resources to have only one counterpart in every target ontology.
   *
   * First strategy: delete all links which are involved in 1:n mappings
   * @param graph input graph
   * @return output graph
   * @throws Exception
   */
  public static Graph<Long, FlinkVertex, NullValue> applyLinkFilterStrategy(Graph<Long, FlinkVertex, NullValue> graph)
      throws Exception {
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

    return Graph.fromDataSet(graph.getVertices(),
        edgesNoDuplicates,
        ExecutionEnvironment.createLocalEnvironment());
  }

  public static Graph<Long, FlinkVertex, NullValue> applyTypePreprocessing(Graph<Long, FlinkVertex, NullValue> graph) {
    DataSet<org.apache.flink.graph.Vertex<Long, FlinkVertex>> vertices = graph.getVertices()
        .map(new MapFunction<org.apache.flink.graph.Vertex<Long, FlinkVertex>, org.apache.flink.graph.Vertex<Long, FlinkVertex>>() {
          @Override
          public org.apache.flink.graph.Vertex<Long, FlinkVertex> map(org.apache.flink.graph.Vertex<Long, FlinkVertex> vertex) throws Exception {
            FlinkVertex flinkVertex = vertex.getValue();

            Map<String, Object> properties = flinkVertex.getProperties();
            if (properties.containsKey(Utils.TYPE)) {
              // get relevant key and translate with custom dictionary for internal use
              Object oldValue = properties.get(Utils.TYPE);
              String resultType;
              if (oldValue instanceof Set) {
                Set<String> values = Sets.newHashSet((Set<String>) oldValue);
                resultType = getDictValue(values);
              } else {
                resultType = getDictValue(Sets.newHashSet((String) oldValue));
              }
              properties.put(Utils.TYPE_INTERN, resultType);
              return vertex;
            }
            return vertex;
          }
        });

    return Graph.fromDataSet(vertices, graph.getEdges(),
        ExecutionEnvironment.createLocalEnvironment());
  }



  private static String getDictValue(Set<String> values) {
    for (String value : values) {
      if (TypeDictionary.PRIMARY_TYPE.containsKey(value)) {
        return TypeDictionary.PRIMARY_TYPE.get(value);
      }
    }

    for (String value : values) {
      if (TypeDictionary.SECONDARY_TYPE.containsKey(value)) {
        return TypeDictionary.SECONDARY_TYPE.get(value);
      }
    }

    for (String value : values) {
      if (TypeDictionary.TERTIARY_TYPE.containsKey(value)) {
        return TypeDictionary.TERTIARY_TYPE.get(value);
      }
    }

    return "-1";
  }
}
