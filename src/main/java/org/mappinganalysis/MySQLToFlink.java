package org.mappinganalysis;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.io.JDBCDataLoader;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.GeoDistance;

import java.util.HashSet;
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

    Graph<Long, FlinkVertex, NullValue> graph = getInputGraph();

    // preprocessing, comment line if not needed
//    graph = applyLinkFilterStrategy(graph);

    DataSet<Triplet<Long, FlinkVertex, NullValue>> baseTriplets = graph.getTriplets();

    FilterOperator<Triplet<Long, FlinkVertex, Double>> geoSimilarity
        = baseTriplets
        .filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimFunction())
        .filter(new GeoCodeThreshold());

    // cc on geo coords
    DataSet<Tuple2<Long, Long>> ccEdges = geoSimilarity.project(0, 1);

    FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents();
    DataSet<Tuple2<Long, Long>> flinkResult = connectedComponents
        .compute(graph.getVertices().map(new CcVerticesCreator()), ccEdges, 1000);

    System.out.println(flinkResult.project(1).distinct().count());
    List<Tuple2<Long, Long>> ccGeoList = flinkResult
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

  public static void getLinksWhereLabelIsEqualExample() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
    JDBCDataLoader loader = new JDBCDataLoader(environment);

    DataSet<Vertex<Long, String>> vertices = loader.getVertices()
        .map(new LabelExtractor());

    DataSet<Edge<Long, NullValue>> edges = loader.getEdges();
    Graph<Long, String, NullValue> graph = Graph.fromDataSet(vertices, edges, environment);

//    // check if each edge points to existing vertices
    // System.out.println(graph.validate(new InvalidVertexIdsValidator<Integer, String, NullValue>()));

    graph.getTriplets()
        .map(new SimilarTripletExtractor())
        .filter(new TripletFilter()).print();
  }

  private static Graph<Long, FlinkVertex, NullValue> applyLinkFilterStrategy(Graph<Long, FlinkVertex, NullValue> graph) throws Exception {

    // TODO EdgeDirection.IN
    DataSet<Edge<Long, NullValue>> deleteEdges = graph.groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.OUT)
        .groupBy(1, 2)
        .aggregate(Aggregations.SUM, 3)
        .filter(new ExcludeOneToManyOntologiesFilter())
        .map(new MapFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>, Edge<Long, NullValue>>() {
          @Override
          public Edge<Long, NullValue> map(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple) throws Exception {
            return tuple.f0;
          }
        });

    return Graph.fromDataSet(graph.getVertices(), deleteEdges, ExecutionEnvironment.createLocalEnvironment());
  }

  private static class JoinFilterStrategyFunction implements JoinFunction<Edge<Long, NullValue>, Edge<Long, NullValue>, Edge<Long, NullValue>> {
    @Override
    public Edge<Long, NullValue> join(Edge<Long, NullValue> edge, Edge<Long, NullValue> deleteEdge) throws Exception {
      return edge;
    }
  }

  private static class ExcludeOneToManyOntologiesFilter implements FilterFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {
    @Override
    public boolean filter(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple) throws Exception {
      return tuple.f3 < 2;
    }
  }

  private static class NeighborOntologyFunction
      implements NeighborsFunctionWithVertexValue<Long, FlinkVertex, NullValue, Tuple4<Edge<Long, NullValue>, Long, String, Integer>> {

    @Override
    public void iterateNeighbors(Vertex<Long, FlinkVertex> vertex,
                                 Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, FlinkVertex>>> neighbors,
                                 Collector<Tuple4<Edge<Long, NullValue>, Long, String, Integer>> collector) throws Exception {

      for (Tuple2<Edge<Long, NullValue>, Vertex<Long, FlinkVertex>> neighbor : neighbors) {
        collector.collect(new Tuple4<>(neighbor.f0, neighbor.f1.getId(), neighbor.f1.getValue().getProperties().get("ontology").toString(), 1));
      }
    }
  }

  private static class OntologyReduce implements GroupReduceFunction<Tuple3<Integer, Integer, String>, Tuple2<Integer, Integer>> {
    @Override
    public void reduce(Iterable<Tuple3<Integer, Integer, String>> in, Collector<Tuple2<Integer, Integer>> out) throws Exception {
      HashSet<String> uniqueStrings = new HashSet<>();
      int count = 0;
      int src = 0;
      for (Tuple3<Integer, Integer, String> tuple : in) {
        src = tuple.f0;
        if (!uniqueStrings.add(tuple.f2)) {
          ++count;
        }
      }
      out.collect(new Tuple2<>(src, count));
    }
  }

  private static class OntologyExtractor implements MapFunction<FlinkVertex, Vertex<Integer, String>> {
    @Override
    public Vertex<Integer, String> map(FlinkVertex flinkVertex) throws Exception {

      String ontology = flinkVertex.getValue().toString();
      return new Vertex<>(Ints.checkedCast(flinkVertex.getId()), ontology);
    }
  }

  private static class LabelExtractor implements MapFunction<FlinkVertex, Vertex<Long, String>> {
    @Override
    public Vertex<Long, String> map(FlinkVertex flinkVertex) throws Exception {
      Object label = flinkVertex.getValue().get("label");
      return new Vertex<>(flinkVertex.getId(), (label != null) ? label.toString() : "null");
    }
  }

  private static class TripletFilter implements FilterFunction<Triplet<Long, String, Float>> {
    @Override
    public boolean filter(Triplet<Long, String, Float> weightedTriplet) throws Exception {
      return weightedTriplet.getEdge().getValue() == 1f;
    }
  }

  private static class GeoCodeThreshold implements FilterFunction<Triplet<Long, FlinkVertex, Double>> {
    @Override
    public boolean filter(Triplet<Long, FlinkVertex, Double> distanceThreshold) throws Exception {
      return distanceThreshold.getEdge().getValue() < 50000;
    }
  }

  private static class CcVerticesCreator implements MapFunction<Vertex<Long, FlinkVertex>, Long> {
    @Override
    public Long map(Vertex<Long, FlinkVertex> flinkVertex) throws Exception {
      return flinkVertex.getId();
    }
  }

  /**
   * Return similarity 1f if labels of two resources are equal.
   */
  private static class SimilarTripletExtractor implements MapFunction<Triplet<Long, String, NullValue>,
      Triplet<Long, String, Float>> {
    @Override
    public Triplet<Long, String, Float> map(Triplet<Long, String, NullValue> triplet) throws Exception {
      boolean isSimilar = triplet.getSrcVertex().getValue().toLowerCase()
          .equals(triplet.getTrgVertex().getValue().toLowerCase());
      return new Triplet<>(
          triplet.getSrcVertex(),
          triplet.getTrgVertex(),
          new Edge<>(
              triplet.getSrcVertex().getId(),
              triplet.getTrgVertex().getId(),
              (isSimilar) ? 1f : 0f));
    }
  }

  /**
   * Return triple including the distance between 2 geo points as edge value.
   */
    private static class GeoCodeSimFunction implements MapFunction<Triplet<Long, FlinkVertex, NullValue>,
          Triplet<Long, FlinkVertex, Double>> {

    @Override
    public Triplet<Long, FlinkVertex, Double> map(Triplet<Long,
        FlinkVertex, NullValue> triplet) throws Exception {
      Map<String, Object> source = triplet.getSrcVertex().getValue().getProperties();
      Map<String, Object> target = triplet.getTrgVertex().getValue().getProperties();


      double distance = GeoDistance.distance(getDouble(source.get("lat")),
          getDouble(source.get("lon")),
          getDouble(target.get("lat")),
          getDouble(target.get("lon")));

      return new Triplet<>(
          triplet.getSrcVertex(),
          triplet.getTrgVertex(),
          new Edge<>(triplet.getSrcVertex().getId(),
          triplet.getTrgVertex().getId(), distance));
    }

    private Double getDouble(Object latlon) {
      // TODO how to handle multiple values in lat/lon correctly?

      if (latlon instanceof List) {
        return Doubles.tryParse(((List) latlon).get(0).toString());
      } else {
        return Doubles.tryParse(latlon.toString());
      }
    }
  }

  /**
   * Filter coordinates where both latitude and longitude are 0 for either source or target resource.
   */
  private static class EmptyGeoCodeFilter implements FilterFunction<Triplet<Long, FlinkVertex, NullValue>> {
    @Override
    public boolean filter(Triplet<Long, FlinkVertex, NullValue> triplet) throws Exception {

      Map<String, Object> source = triplet.getSrcVertex().getValue().getProperties();
      Map<String, Object> target = triplet.getTrgVertex().getValue().getProperties();

      return isGeoPoint(source) && isGeoPoint(target);
    }

    private boolean isGeoPoint(Map<String, Object> props) {
      if (props.containsKey("lat") && props.containsKey("lon")) {
        Object lat = props.get("lat");
        Object lon = props.get("lon");
        // TODO how to handle multiple values in lat/lon correctly?
        return ((getDouble(lat) == null) || (getDouble(lon) == null)) ? Boolean.FALSE : Boolean.TRUE;
      } else {
        return Boolean.FALSE;
      }
    }

    private Double getDouble(Object latlon) {
      if (latlon instanceof List) {
        return Doubles.tryParse(((List) latlon).get(0).toString());
      } else {
        return Doubles.tryParse(latlon.toString());
      }
    }
  }
}
