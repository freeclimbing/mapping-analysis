package org.mappinganalysis;

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
import org.mappinganalysis.io.JDBCDataLoader;
import org.mappinganalysis.model.FlinkVertex;

import java.util.HashSet;
import java.util.ResourceBundle;

/**
 * Read data from MySQL database via JDBC into Apache Flink.
 */
public class MySQLToFlink {
  private static final Logger LOG = Logger.getLogger(MySQLToFlink.class);
  private final ExecutionEnvironment env;
  private final ResourceBundle prop;

  public MySQLToFlink(ExecutionEnvironment env, ResourceBundle prop) {
    this.env = env;
    this.prop = prop;
  }

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
    JDBCDataLoader loader = new JDBCDataLoader(environment);

    DataSet<FlinkVertex> vertices = loader.getVertices();

    DataSet<Edge<Integer, NullValue>> edges = loader.getEdges();
    Graph graph = Graph.fromDataSet(vertices, edges, environment);

  }

  private static void printVerticesWithUnsimilarNames() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();
    JDBCDataLoader loader = new JDBCDataLoader(environment);

    DataSet<Vertex<Integer, String>> vertices = loader.getVertices()
        .map(new LabelExtractor());

//
//    // exclude null labels
//    flinkVertexDataSet = flinkVertexDataSet.filter(new FilterFunction<FlinkVertex>() {
//      @Override
//      public boolean filter(FlinkVertex vertex) throws Exception {
//        return vertex.getProperties().get("label") != null;
//      }
//    });

    DataSet<Edge<Integer, NullValue>> edges = loader.getEdges();
    Graph<Integer, String, NullValue> graph = Graph.fromDataSet(vertices, edges, environment);

//    // check if each edge points to existing vertices
    // System.out.println(graph.validate(new InvalidVertexIdsValidator<Integer, String, NullValue>()));
//
    graph.getTriplets()
        .map(new TripletExtractor())
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

    public Vertex<Integer, String> map(FlinkVertex flinkVertex) throws Exception {
      String ontology = flinkVertex.getProperties().get("ontology").toString();
      return new Vertex<>(flinkVertex.getVertexId(), ontology);
    }
  }

  private static class LabelExtractor implements MapFunction<FlinkVertex, Vertex<Integer, String>> {

    public Vertex<Integer, String> map(FlinkVertex flinkVertex) throws Exception {
      Object label = flinkVertex.getProperties().get("label");
      return new Vertex<>(flinkVertex.getVertexId(), (label != null) ? label.toString() : "null");
    }
  }

  private static class TripletFilter implements FilterFunction<Triplet<Integer, String, Float>> {

    public boolean filter(Triplet<Integer, String, Float> weightedTriplet) throws Exception {
      return weightedTriplet.getEdge().getValue() == 0f;
    }  }

  private static class TripletExtractor implements MapFunction<Triplet<Integer, String, NullValue>, Triplet<Integer, String, Float>> {

    public Triplet<Integer, String, Float> map(Triplet<Integer, String, NullValue> triplet) throws Exception {
      boolean isSimilar = triplet.getSrcVertex().getValue().toLowerCase().equals(triplet.getTrgVertex().getValue().toLowerCase());
      return new Triplet<>(
          triplet.getSrcVertex(),
          triplet.getTrgVertex(),
          new Edge<>(
              triplet.getSrcVertex().getId(),
              triplet.getTrgVertex().getId(),
              (isSimilar) ? 1f : 0f));
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
}
