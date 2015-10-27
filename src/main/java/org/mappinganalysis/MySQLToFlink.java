package org.mappinganalysis;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
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
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.utils.HaversineGeoDistance;

import java.util.HashSet;
import java.util.Map;

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

    DataSet<FlinkVertex> inputVertices = loader.getVertices();
    DataSet<Edge<Long, NullValue>> edges = loader.getEdges();
    // preprocessing TODO restrict edges strategy

    DataSet<Vertex<Long, FlinkVertex>> vertices = inputVertices.map(new MapFunction<FlinkVertex, Vertex<Long, FlinkVertex>>() {
      @Override
      public Vertex<Long, FlinkVertex> map(FlinkVertex flinkVertex) throws Exception {
        return new Vertex<>(flinkVertex.getId(), flinkVertex);
      }
    });

    Graph<Long, FlinkVertex, NullValue> graph = Graph.fromDataSet(vertices, edges, environment);
    graph.getTriplets().print();
    FilterOperator<Triplet<Long, FlinkVertex, Double>> filter
        = graph.getTriplets()
        // .filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimFunction())
        .filter(new GeoCodeThreshold());

    filter.print();

    DataSet<Tuple2<Long, Long>> ccEdges = filter.project(0, 1);

//    // cc on geo coords
    FlinkConnectedComponents connectedComponents = new FlinkConnectedComponents();
    DataSet<Tuple2<Long, Long>> flinkResult = connectedComponents
        .compute(vertices.map(new CcVerticesCreator()), ccEdges, 1000);
//
    flinkResult.print();
//
    flinkResult
        .groupBy(1)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void reduce(Iterable<Tuple2<Long, Long>> component, Collector<Tuple2<Long, Long>> out) throws Exception {
            long count = 0;
            long id = 0;
            for (Tuple2<Long, Long> vertex : component) {
              count++;
              id = vertex.f1;
            }
            out.collect(new Tuple2<>(id, count));
          }
        });
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
      return distanceThreshold.getEdge().getValue() < 1000;
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
      GeoCode s = getGeoCode(triplet.getSrcVertex().getValue());
      GeoCode t = getGeoCode(triplet.getTrgVertex().getValue());

      double distance = HaversineGeoDistance.distance(s.getLat(), s.getLon(), t.getLat(), t.getLon());

//      HashMap<String, Object> hashMap = new HashMap<>();
//      hashMap.put("distance", distance);
//      reuseProp.setPropertyValue(hashMap);

      return new Triplet<>(
          triplet.getSrcVertex(),
          triplet.getTrgVertex(),
          new Edge<>(triplet.getSrcVertex().getId(),
          triplet.getTrgVertex().getId(), distance));
    }

    /**
     * Get GeoCode for a vertex.
     * @param vertexProps vertex properties
     * @return
     */
    private GeoCode getGeoCode(FlinkVertex vertexProps) {
      Map<String, Object> props = vertexProps.getProperties();

      Double longitude = null;
      Double latitude = null;

      if (props.containsKey("lat") && props.containsKey("lon")) {
        latitude = Doubles.tryParse(props.get("lat").toString());
        longitude = Doubles.tryParse(props.get("lon").toString());
      }

      return new GeoCode((latitude != null) ? (double) latitude : 0,
          (longitude != null) ? (double) longitude : 0);

    }
  }

//  private static class GeoSimFunction extends SimilarityFunction<Triplet<Long, PropertyContainer, Double>> {
//
//    public Triplet<Long, PropertyContainer, Double> map(Triplet<Long, PropertyContainer,
//        NullValue> triplet) throws Exception {
//      triplet.getSrcVertex().getValue().get("lat")
//      return null;
//    }
//  }

//  ccprivate static class CcVerticesCreator implements MapFunction<Vertex<Long, PropertyContainer>, Long> {
//    @Override
//    public Long map(Vertex<Long, PropertyContainer> flinkVertex) throws Exception {
//      return flinkVertex.getId();
//    }
//  }

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
        Object latitude = flinkVertex.getProperties().get("lat");
        Object longitude = flinkVertex.getProperties().get("lon");
        GeoCode geoCode = new GeoCode((latitude != null) ? (double) latitude : 0,
            (longitude != null) ? (double) longitude : 0);
        return new Vertex<>(flinkVertex.getId(), geoCode);
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

//  private static class VertexExtractor implements MapFunction<FlinkVertex, Vertex<Long, PropertyContainer>> {
//    @Override
//    public Vertex<Long, PropertyContainer> map(FlinkVertex flinkVertex) throws Exception {
//      return new Vertex<>(flinkVertex.getId(), flinkVertex.getValue());
//    }
//  }
}
