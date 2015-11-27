package org.mappinganalysis.graph;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;
import org.mappinganalysis.MySQLToFlink;
import org.mappinganalysis.utils.Utils;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransitiveClosureTest {

  @Test
  public void simpleTransitiveClosureTest() throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.createLocalEnvironment();

    List<Tuple2<Long, Long>> input = Lists.newArrayList();
    input.add(new Tuple2<>(3L, 1L));
    input.add(new Tuple2<>(3L, 2L));
    input.add(new Tuple2<>(3L, 4L));
    input.add(new Tuple2<>(5L, 6L));
    input.add(new Tuple2<>(5L, 7L));
    DataSet<Tuple2<Long, Long>> testData = environment.fromCollection(input);

    DataSet<Tuple2<Long, Long>> result = TransitiveClosureNaive.compute(testData, 10);
    assertEquals(25, result.count());

    result = TransitiveClosureNaive.restrictToNewEdges(testData, result);
    assertEquals(4, result.count());

    List<Tuple2<Long, Long>> resultList = result.collect();
    assertTrue(resultList.contains(new Tuple2<>(6L, 7L)));
    assertTrue(resultList.contains(new Tuple2<>(1L, 4L)));
    assertTrue(resultList.contains(new Tuple2<>(1L, 2L)));
    assertTrue(resultList.contains(new Tuple2<>(2L, 4L)));

    // GEO OAEI dataset
    DataSet<Tuple2<Long, Long>> edges = MySQLToFlink
        .getInputGraph(Utils.GEO_FULL_NAME)
        .getEdges()
        .project(0, 1);
    DataSet<Tuple2<Long, Long>> geoResult = TransitiveClosureNaive.compute(edges, 10);
    assertEquals(29908, geoResult.count());
    DataSet<Tuple2<Long, Long>> geoFinalResult = TransitiveClosureNaive.restrictToNewEdges(edges, geoResult);
    assertEquals(5557, geoFinalResult.count());


    // needs much time (~90s)
//    DataSet<Tuple2<Long, Long>> edgesBig = MySQLToFlink
//        .getInputGraph(Utils.LL_FULL_NAME)
//        .getEdges()
//        .project(0, 1);
//    DataSet<Tuple2<Long, Long>> geo1Result = TransitiveClosureNaive.compute(edgesBig, 10);
//    long one = geo1Result.count();
//    DataSet<Tuple2<Long, Long>> geo1FinalResult = TransitiveClosureNaive.restrictToNewEdges(edgesBig, geo1Result);
//    long oneFinal = geo1FinalResult.count();
//
//    DataSet<Tuple2<Long, Long>> geo2Result = TransitiveClosureNaive.compute(edgesBig, 100);
//    long two = geo2Result.count();
//    DataSet<Tuple2<Long, Long>> geo2FinalResult = TransitiveClosureNaive.restrictToNewEdges(edgesBig, geo2Result);
//    long twoFinal = geo2FinalResult.count();
//
//    DataSet<Tuple2<Long, Long>> geo3Result = TransitiveClosureNaive.compute(edgesBig, 1000);
//    long three = geo3Result.count();
//    DataSet<Tuple2<Long, Long>> geo3FinalResult = TransitiveClosureNaive.restrictToNewEdges(edgesBig, geo3Result);
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
