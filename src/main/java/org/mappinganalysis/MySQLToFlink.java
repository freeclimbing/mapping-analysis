package org.mappinganalysis;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.io.JDBCDataLoader;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.model.functions.EmptyGeoCodeFilter;
import org.mappinganalysis.model.functions.GeoCodeSimFunction;
import org.mappinganalysis.model.functions.NeighborOntologyFunction;
import org.mappinganalysis.model.functions.SimilarTripletExtractor;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Read data from MySQL database via JDBC into Apache Flink.
 */
public class MySQLToFlink {
  private static final Logger LOG = Logger.getLogger(MySQLToFlink.class);

  public MySQLToFlink() {
  }

  public static void main(String[] args) throws Exception {
    // check if each edge points to existing vertices
    // System.out.println(graph.validate(new InvalidVertexIdsValidator<Integer, String, NullValue>()));
    Graph<Long, FlinkVertex, NullValue> graph = getInputGraph();

    // preprocessing, comment line if not needed
    graph = applyLinkFilterStrategy(graph);

    // 2 similarity functions, similarities are added as edge value and merged (if more than one similarity)
    DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets = graph.getTriplets();

    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> geoSimilarity
        = baseTriplets
        .filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimFunction())
        .filter(new GeoCodeThreshold());

    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> exactSim
        = baseTriplets
        .map(new SimilarTripletExtractor())
        .filter(new TripletFilter());

    DataSet<Triplet<Long, FlinkVertex, Map<String, Object>>> joinedSimValues = geoSimilarity.fullOuterJoin(exactSim)
        .where(0, 1)
        .equalTo(0, 1)
        .with(new JoinSimilarityValueFunction());

//    joinedSimValues.filter(new FilterSizeMinTwo()).print();

    // cc on geo coords
//    DataSet<Tuple2<Long, Long>> geoEdges = geoSimilarity.project(0, 1);
//    DataSet<Tuple2<Long, Long>> labelEdges = exactSim.project(0, 1);
    DataSet<Tuple2<Long, Long>> ccEdges = joinedSimValues.project(0, 1);


    DataSet<Long> ccVertices = graph.getVertices().map(new CcVerticesCreator());

    FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents();
    DataSet<Tuple2<Long, Long>> ccResult = connectedComponents
        .compute(ccVertices, ccEdges, 1000);

//
//
//
//    //???
//    DataSet<Vertex<Long, FlinkVertex>> vertices = graph.getVertices();
//    vertices
//        .map(new MapFunction<Vertex<Long, FlinkVertex>, Vertex<Long, FlinkVertex>>() {
//          @Override
//          public Vertex<Long, FlinkVertex> map(Vertex<Long, FlinkVertex> longFlinkVertexVertex) throws Exception {
//            return null;
//          }
//        });
//
////    Graph.fromDataSet(graph.getVertices(),
////        edges, ExecutionEnvironment.createLocalEnvironment());
//
        countPrintResourcesPerCc(ccResult);
  }

  /**
   * Count resources per component for a given flink connected component result set.
   * @param ccResult dataset to be analyzed
   * @throws Exception
   */
  private static void countPrintResourcesPerCc(DataSet<Tuple2<Long, Long>> ccResult) throws Exception {
    System.out.println(ccResult.project(1).distinct().count());
    List<Tuple2<Long, Long>> ccGeoList = ccResult
        .groupBy(1)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void reduce(Iterable<Tuple2<Long, Long>> component, Collector<Tuple2<Long, Long>> out) throws Exception {
            long count = 0;
            long id = 0;
            for (Tuple2<Long, Long> vertex : component) {
              if (vertex.f1 == 4794 || vertex.f1 == 5680) {
                System.out.println(vertex);
              }
              count++;
              id = vertex.f1;
            }
            out.collect(new Tuple2<>(id, count));
          }
        })
        .collect();

    int one = 0;
    int two = 0;
    int three = 0;
    int four = 0;
    int five = 0;
    int six = 0;
    int seven = 0;
    for (Tuple2<Long, Long> tuple2 : ccGeoList) {
      if (tuple2.f1 == 1) {
        one++;
      } else if (tuple2.f1 == 2) {
        two++;
      } else if (tuple2.f1 == 3) {
        three++;
      } else if (tuple2.f1 == 4) {
        four++;
      } else if (tuple2.f1 == 5) {
        five++;
      } else if (tuple2.f1 == 6) {
        six++;
      } else if (tuple2.f1 == 7) {
        seven++;
      }
    }
    System.out.println("one: " + one + " two: " + two + " three: " + three +
        " four: " + four + " five: " + five + " six: " + six + " seven: " + seven);
  }

  /**
   * Create the input graph for further analysis.
   * @return graph with vertices and edges.
   * @throws Exception
   */
  private static Graph<Long, FlinkVertex, NullValue> getInputGraph() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
    JDBCDataLoader loader = new JDBCDataLoader(environment);

    DataSet<FlinkVertex> inputVertices = loader.getVertices();
    DataSet<Edge<Long, NullValue>> edges = loader.getEdges();

    DataSet<Vertex<Long, FlinkVertex>> vertices = inputVertices
        .map(new MapFunction<FlinkVertex, Vertex<Long, FlinkVertex>>() {
          @Override
          public Vertex<Long, FlinkVertex> map(FlinkVertex flinkVertex) throws Exception {
            return new Vertex<>(flinkVertex.getId(), flinkVertex);
          }
        });

    return Graph.fromDataSet(vertices, edges, environment);
  }

  /**
   * Preprocessing strategy to restrict resources to have only one counterpart in every target ontology.
   *
   * First strategy: delete all links which are involved in 1:n mappings
   * @param graph input graph
   * @return output graph
   * @throws Exception
   */
  private static Graph<Long, FlinkVertex, NullValue> applyLinkFilterStrategy(Graph<Long, FlinkVertex, NullValue> graph)
      throws Exception {

    // TODO EdgeDirection.IN
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

  public static class FilterExactMatch implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
    @Override
    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> triplet) throws Exception {
      return triplet.getEdge().getValue().containsKey("exactMatch");
    }
  }

  public static class FilterSizeMinTwo implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
    @Override
    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> triplet) throws Exception {
      return triplet.getEdge().getValue().size() > 1;
    }
  }

  private static class JoinFilterStrategyFunction
      implements JoinFunction<Edge<Long, NullValue>, Edge<Long, NullValue>, Edge<Long, NullValue>> {
    @Override
    public Edge<Long, NullValue> join(Edge<Long, NullValue> edge, Edge<Long, NullValue> deleteEdge) throws Exception {
      return edge;
    }
  }

  private static class ExcludeOneToManyOntologiesFilter
      implements FilterFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {
    @Override
    public boolean filter(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple) throws Exception {
      return tuple.f3 < 2;
    }
  }

  private static class LabelExtractor implements MapFunction<FlinkVertex, Vertex<Long, String>> {
    @Override
    public Vertex<Long, String> map(FlinkVertex flinkVertex) throws Exception {
      Object label = flinkVertex.getValue().get("label");
      return new Vertex<>(flinkVertex.getId(), (label != null) ? label.toString() : "null");
    }
  }

  private static class JoinSimilarityValueFunction
      implements JoinFunction<Triplet<Long, FlinkVertex, Map<String, Object>>,
      Triplet<Long, FlinkVertex, Map<String, Object>>,
      Triplet<Long, FlinkVertex, Map<String, Object>>> {
    @Override
    public Triplet<Long, FlinkVertex, Map<String, Object>> join(
        Triplet<Long, FlinkVertex, Map<String, Object>> t1,
        Triplet<Long, FlinkVertex, Map<String, Object>> t2) throws Exception {

      Vertex<Long, FlinkVertex> source;
      Vertex<Long, FlinkVertex> target;
      if (t1 != null) {
        source = t1.getSrcVertex();
        target = t1.getTrgVertex();
      } else {
        source = t2.getSrcVertex();
        target = t2.getTrgVertex();
      }

      Map<String, Object> result = Maps.newHashMap();
      if (t1 != null) {
        result.putAll(t1.getEdge().getValue());
      }
      if (t2 != null) {
        result.putAll(t2.getEdge().getValue());
      }

      return new Triplet<>(
          source,
          target,
          new Edge<>(
              source.getId(),
              target.getId(),
              result));
    }
  }

  private static class TripletFilter implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
    @Override
    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> weightedTriplet) throws Exception {
      Map<String, Object> props = weightedTriplet.getEdge().getValue();
      return props.containsKey("exactMatch") && (float) props.get("exactMatch") == 1f;
    }
  }

  /**
   * TODO Threshold needs to be flexible.
   */
  private static class GeoCodeThreshold
      implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
    @Override
    public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> distanceThreshold) throws Exception {
      return ((double) distanceThreshold.getEdge().getValue().get("distance")) < 50000;
    }
  }

  private static class CcVerticesCreator implements MapFunction<Vertex<Long, FlinkVertex>, Long> {
    @Override
    public Long map(Vertex<Long, FlinkVertex> flinkVertex) throws Exception {
      return flinkVertex.getId();
    }
  }
}
