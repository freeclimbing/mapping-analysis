package org.mappinganalysis.graph;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterComputationTest {

  @Test
  public void computeMissingEdgesTest() throws Exception {
    List<Edge<Long, NullValue>> edgeList = Lists.newArrayList();
    edgeList.add(new Edge<>(5680L, 5681L, NullValue.getInstance()));
    edgeList.add(new Edge<>(5680L, 5984L, NullValue.getInstance()));
    Graph<Long, NullValue, NullValue> graph = Graph.fromCollection(edgeList, ExecutionEnvironment.createLocalEnvironment());

    DataSet<Vertex<Long, FlinkVertex>> inputVertices = graph
        .getVertices()
        .map(new MapFunction<Vertex<Long, NullValue>, Vertex<Long, FlinkVertex>>() {
          @Override
          public Vertex<Long, FlinkVertex> map(Vertex<Long, NullValue> vertex) throws Exception {
            Map<String, Object> prop = Maps.newHashMap();
            prop.put(Utils.CC_ID, 5680L);

            return new Vertex<>(vertex.getId(), new FlinkVertex(vertex.getId(), prop));
          }
        });

    DataSet<Edge<Long, NullValue>> allEdges
        = ClusterComputation.computeComponentEdges(inputVertices);

    assertEquals(9, allEdges.count());

    DataSet<Edge<Long, NullValue>> newEdges
        = ClusterComputation.restrictToNewEdges(graph.getEdges(), allEdges);
    assertEquals(1, newEdges.count());
    assertTrue(newEdges.collect().contains(new Edge<>(5681L, 5984L, NullValue.getInstance())));
  }

  @Test
  public void simpleTransitiveClosureTest() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

    List<Tuple2<Long, Long>> input = Lists.newArrayList();
    input.add(new Tuple2<>(3L, 1L));
    input.add(new Tuple2<>(3L, 2L));
    input.add(new Tuple2<>(3L, 4L));
    input.add(new Tuple2<>(6L, 5L));
    input.add(new Tuple2<>(7L, 5L));

    DataSet<Tuple2<Long, Long>> testData = environment.fromCollection(input);


    DataSet<Tuple2<Long, Long>> result = ClusterComputation.computeComponentEdgesAsTuple2(testData, 5);
    assertEquals(25, result.count());
    result.print();

    result = ClusterComputation.restrictToNewTuples(testData, result);
    assertEquals(4, result.count());

    List<Tuple2<Long, Long>> resultList = result.collect();
    assertTrue(resultList.contains(new Tuple2<>(6L, 7L)));
    assertTrue(resultList.contains(new Tuple2<>(1L, 4L)));
    assertTrue(resultList.contains(new Tuple2<>(1L, 2L)));
    assertTrue(resultList.contains(new Tuple2<>(2L, 4L)));

    // GEO OAEI dataset
    DataSet<Tuple2<Long, Long>> edges = MappingAnalysisExample
        .getInputGraph(Utils.GEO_FULL_NAME, environment)
        .getEdges()
        .project(0, 1);
    DataSet<Tuple2<Long, Long>> geoResult = ClusterComputation.computeComponentEdgesAsTuple2(edges, 10);
    assertEquals(29908, geoResult.count());
    DataSet<Tuple2<Long, Long>> geoFinalResult = ClusterComputation.restrictToNewTuples(edges, geoResult);
    assertEquals(5557, geoFinalResult.count());


    // needs much time (~90s)
//    DataSet<Tuple2<Long, Long>> edgesBig = MySQLToFlink
//        .getInputGraph(Utils.LL_FULL_NAME)
//        .getEdges()
//        .project(0, 1);
//    DataSet<Tuple2<Long, Long>> geo1Result = TransitiveClosureNaive.naiveCompute(edgesBig, 10);
//    long one = geo1Result.count();
//    DataSet<Tuple2<Long, Long>> geo1FinalResult = TransitiveClosureNaive.restrictToNewTuples(edgesBig, geo1Result);
//    long oneFinal = geo1FinalResult.count();
//
//    DataSet<Tuple2<Long, Long>> geo2Result = TransitiveClosureNaive.naiveCompute(edgesBig, 100);
//    long two = geo2Result.count();
//    DataSet<Tuple2<Long, Long>> geo2FinalResult = TransitiveClosureNaive.restrictToNewTuples(edgesBig, geo2Result);
//    long twoFinal = geo2FinalResult.count();
//
//    DataSet<Tuple2<Long, Long>> geo3Result = TransitiveClosureNaive.naiveCompute(edgesBig, 1000);
//    long three = geo3Result.count();
//    DataSet<Tuple2<Long, Long>> geo3FinalResult = TransitiveClosureNaive.restrictToNewTuples(edgesBig, geo3Result);
//    long threeFinal = geo3FinalResult.count();
//
//    // one: 6243577 1083144
//    // two: 2907119 425711
//    // three: 2907119 287313
//    System.out.println("one: " + one + " " + oneFinal + "\n"
//        + " two: " + two + " " + twoFinal + "\n"
//        + " three: " + three + " " + threeFinal);
  }

}
