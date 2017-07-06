package org.mappinganalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.IsolatedEdgeRemover;
import org.mappinganalysis.util.Constants;

import java.util.List;

/**
 * basic test class
 * @deprecated
 */
public class BasicTest {

  /**
   * Restrict graph for testing purpose. First 500 vertices and contained edges.
   *
   * not currently in use
   */
  @Deprecated
  private static Graph<Long, ObjectMap, NullValue> restrictGraph(Graph<Long, ObjectMap, NullValue> graph,
                                                                 ExecutionEnvironment env) {
    // restrict to first ??? clusters
    DataSet<Tuple1<Long>> restrictedComponentIds = graph.getVertices()
        .map(vertex -> new Tuple1<>((long) vertex.getValue().get(Constants.CC_ID)))
        .returns(new TypeHint<Tuple1<Long>>() {})
//        .filter(tuple -> {
//          return tuple.f0 == 1868L;
////            return tuple.f0 == 1134L || tuple.f0 == 60L;// || tuple.f0 == 1135L || tuple.f0 == 8214L; // typegroupby diff
////            return tuple.f0 == 890L || tuple.f0 == 1134L || tuple.f0 == 60L || tuple.f0 == 339L; // typegroupby diff
//        });
        .first(500);

    DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
        .map(vertex -> new Tuple2<>(vertex.getId(), (long) vertex.getValue().get(Constants.CC_ID))) //vid, ccid
        .returns(new TypeHint<Tuple2<Long, Long>>() {})
        .join(restrictedComponentIds)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<Long, Long> left, Tuple1<Long> right, Collector<Tuple1<Long>> collector)
              throws Exception {
            collector.collect(new Tuple1<>(left.f0));
          }
        })
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with((longTuple1, vertex) -> vertex).returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Edge<Long, NullValue>> newEdges = graph.getEdges()
        .runOperation(new IsolatedEdgeRemover<>(newVertices));

    graph = Graph.fromDataSet(newVertices.distinct(0), newEdges.distinct(0,1), env);
    return graph;
  }

  // todo  test for objectmap not twice lat or lon
//  public void addProperty(String key, Object value) {
//
//    Preconditions.checkArgument(!(key.equals(Utils.LAT) && map.containsKey(Utils.LAT))
//            || !(key.equals(Utils.LON) && map.containsKey(Utils.LON)),
//        map.get(Utils.LAT) + " - " + map.get(Utils.LON) + " LAT or LON already there, new: "
//            + key + ": " + value.toString());

  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void aggregateTest() throws Exception {
    ObjectMap mapOne = new ObjectMap(Constants.GEO);
    mapOne.put("distance", 1.0);
    mapOne.put("trigramSim", 0.738549);
    mapOne.put("aggSimValue", 0.8692745);
    ObjectMap mapTwo = new ObjectMap(Constants.GEO);
    mapTwo.put("distance", 1.0);
    mapTwo.put("trigramSim", 0.957427);
    mapTwo.put("aggSimValue", 0.9787135);
    Edge<Long, ObjectMap> one = new Edge<>(2338L, 3186L, new ObjectMap(mapOne));
    Edge<Long, ObjectMap> two = new Edge<>(1429L, 3186L, new ObjectMap(mapTwo));
    DataSource<Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long>> data = env
        .fromElements(
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(two, 3186L, "http://data.nytimes.com/", 1, 0.9787135, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L),
            new Tuple6<>(one, 3186L, "http://data.nytimes.com/", 1, 0.8692745, 1429L));

    AggregateOperator<Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long>> result = data
        .groupBy(1, 2)
        .sum(3).andMax(4);

    result.print();
  }

  @Test
  public void simpleTest() throws Exception {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> tmpGraph = Graph.fromCollection(edgeList, env);

    DataSet<Vertex<Long, Long>> vertices = tmpGraph.getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, Long>>() {
          @Override
          public Vertex<Long, Long> map(Vertex<Long, NullValue> value) throws Exception {
            return new Vertex<>(value.getId(), value.getId());
          }
        });

    DataSet<Edge<Long, NullValue>> edges = tmpGraph.getEdges()
        .map(edge -> new Edge<>(edge.getSource(), edge.getTarget(), NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {});

    Graph<Long, Long, NullValue> workingGraph = Graph.fromDataSet(vertices, edges, env);

    DataSet<Tuple2<Long, Long>> verticesWithMinIds = workingGraph
        .run(new GSAConnectedComponents<>(1000))
        .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
            return new Tuple2<>(vertex.getId(), vertex.getValue());

          }
        });

    verticesWithMinIds.print();
  }
}