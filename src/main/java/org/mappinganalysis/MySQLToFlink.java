package org.mappinganalysis;

import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
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
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.utils.HaversineGeoDistance;

import java.util.HashSet;

/**
 * Read data from MySQL database via JDBC into Apache Flink.
 */
public class MySQLToFlink {
  private static final Logger LOG = Logger.getLogger(MySQLToFlink.class);

  public MySQLToFlink() {
  }

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
    JDBCDataLoader loader = new JDBCDataLoader(environment);

    DataSet<FlinkVertex> vertices = loader.getVertices();
    DataSet<Edge<Long, NullValue>> edges = loader.getEdges();

    // TODO fix this
//    Graph<Long, PropertyContainer, NullValue> graph1 = Graph.fromDataSet(vertices, edges, environment);

    Graph<Long, GeoCode, NullValue> graph = Graph.fromDataSet(vertices.map(new GeoCodeExtractor()), edges, environment);
//    graph.getVertices().print();

    long count = graph.getTriplets().filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimFunction()).filter(new GeoCodeThreshold()).count();
    System.out.println(count);
    // cc on geo coords
//    DataSet<Tuple2<Long, Long>> ccEdges = edges.project(0, 1);//.print();
//    FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents();
//    DataSet<Tuple2<Long, Long>> flinkResult = connectedComponents.compute(vertices.map(new CcVerticesCreator()), ccEdges, 100);
//    flinkResult.print();
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
  private static void getVerticesExcludeOneToMany(Graph<Integer, String, NullValue> graph) throws Exception {
//    DataSet<Vertex<Integer, String>> vertices = loader.getVertices()
//        .map(new OntologyExtractor());
//    DataSet<Edge<Integer, NullValue>> edges = loader.getEdges();
//    Graph<Integer, String, NullValue> graph = Graph.fromDataSet(vertices, edges, environment);

    graph.groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.OUT)
        .map(new Tuple4Mapper())
        .groupBy(1, 2)
        .aggregate(Aggregations.SUM, 3)
        .filter(new ExcludeOneToManyOntologiesFilter())
        .print();
  }

  private static class ExcludeOneToManyOntologiesFilter implements FilterFunction<Tuple4<Integer, Integer, String, Integer>> {
    @Override
    public boolean filter(Tuple4<Integer, Integer, String, Integer> tuple) throws Exception {
      return tuple.f3 < 2;
    }
  }

  private static class Tuple4Mapper implements MapFunction<Tuple3<Integer, Integer, String>, Tuple4<Integer, Integer, String, Integer>> {
    @Override
    public Tuple4<Integer, Integer, String, Integer> map(Tuple3<Integer, Integer, String> tuple) throws Exception {
      return new Tuple4<>(tuple.f0, tuple.f1, tuple.f2, 1);
    }
  }

  private static class OntologyExtractor implements MapFunction<FlinkVertex, Vertex<Integer, String>> {
    @Override
    public Vertex<Integer, String> map(FlinkVertex flinkVertex) throws Exception {
      String ontology = flinkVertex.getValue().get("ontology").toString();
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

  private static class GeoCodeThreshold implements FilterFunction<Triplet<Long, GeoCode, Double>> {
    @Override
    public boolean filter(Triplet<Long, GeoCode, Double> distanceThreshold) throws Exception {
      return distanceThreshold.getEdge().getValue() < 1;
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
  private static class GeoCodeSimFunction implements MapFunction<Triplet<Long, GeoCode, NullValue>, Triplet<Long, GeoCode, Double>> {
    @Override
    public Triplet<Long, GeoCode, Double> map(Triplet<Long, GeoCode, NullValue> triplet) throws Exception {
      GeoCode s = triplet.getSrcVertex().getValue();
      GeoCode t = triplet.getTrgVertex().getValue();
      double distance = HaversineGeoDistance.distance(s.getLat(), s.getLon(), t.getLat(), t.getLon());
      return new Triplet<>(
          triplet.getSrcVertex(),
          triplet.getTrgVertex(),
          new Edge<>(
              triplet.getSrcVertex().getId(),
              triplet.getTrgVertex().getId(),
              distance));
    }
  }

  private static class NeighborOntologyFunction
      implements NeighborsFunctionWithVertexValue<Integer, String, NullValue, Tuple3<Integer, Integer, String>> {

    @Override
    public void iterateNeighbors(Vertex<Integer, String> vertex,
                                 Iterable<Tuple2<Edge<Integer, NullValue>, Vertex<Integer, String>>> neighbors,
                                 Collector<Tuple3<Integer, Integer, String>> collector) throws Exception {

      for (Tuple2<Edge<Integer, NullValue>, Vertex<Integer, String>> neighbor : neighbors) {
        collector.collect(new Tuple3<>(vertex.getId(), neighbor.f1.getId(), neighbor.f1.getValue()));
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

  private static class GeoCodeExtractor implements MapFunction<FlinkVertex, Vertex<Long, GeoCode>> {

      public Vertex<Long, GeoCode> map(FlinkVertex flinkVertex) throws Exception {
        Object latitude = flinkVertex.getValue().get("lat");
        Object longitude = flinkVertex.getValue().get("lon");
        GeoCode geoCode = new GeoCode((latitude != null) ? (double) latitude : 0,
            (longitude != null) ? (double) longitude : 0);
        return new Vertex<>(flinkVertex.getId(), geoCode);
      }
  }

  private static class CcVerticesCreator implements MapFunction<FlinkVertex, Long> {
    @Override
    public Long map(FlinkVertex flinkVertex) throws Exception {
      return flinkVertex.getId();
    }
  }

  /**
   * Filter coordinates where both latitude and longitude are 0 for either source or target resource.
   */
  private static class EmptyGeoCodeFilter implements FilterFunction<Triplet<Long, GeoCode, NullValue>> {
    @Override
    public boolean filter(Triplet<Long, GeoCode, NullValue> triplet) throws Exception {
      GeoCode s = triplet.getSrcVertex().getValue();
      GeoCode t = triplet.getTrgVertex().getValue();
      return (s.getLat() != 0 && s.getLon() != 0) || (t.getLat() != 0 && t.getLon() != 0);
    }
  }
}
