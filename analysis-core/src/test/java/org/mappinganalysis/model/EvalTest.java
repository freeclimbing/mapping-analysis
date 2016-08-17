package org.mappinganalysis.model;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Stats;
import org.mappinganalysis.util.Utils;

import static org.junit.Assert.assertEquals;

public class EvalTest {
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private static final Logger LOG = Logger.getLogger(EvalTest.class);

  @Test
  public void linksWithIdenticalSourceTest() throws Exception {

    String graphPath = EvalTest.class.getResource("/data/eval/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    DataSet<EdgeIdsVertexValueTuple> result = Stats.getLinksWithSameSource(graph);

    result.print();
  }

  /**
   * Check printEdgeSourceCounts
   * @throws Exception
   */
  @Test
  public void printEdgeSourceCountsTest() throws Exception {
    String graphPath = EvalTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    DataSet<Tuple3<String, String, Integer>> result = Stats.printEdgeSourceCounts(graph);

    result.print();

//    int rowCount = 0;
//    for (Tuple3<String, String, Integer> tuple : result.collect()) {
//      ++rowCount;
//      if (tuple.f0.equals(Constants.GN_NS) && tuple.f1.equals(Constants.LGD_NS)) {
//        assertEquals(4, tuple.f2.intValue());
//      } else if (tuple.f0.equals(Constants.DBP_NS) && tuple.f1.equals(Constants.LGD_NS)) {
//        assertEquals(2, tuple.f2.intValue());
//      } else {
//        assertEquals(1, tuple.f2.intValue());
//      }
//    }
//    assertEquals(6, rowCount);
  }

  @Test
  public void testMissingProps() throws Exception {

    String graphPath = EvalTest.class.getResource("/data/eval/").getFile();

    // 14 entities, 12 with geo, 12 with type
    // 1x geo lat missing
    // 1x geo lon missing
    // 1x type 'bla'
    // 1x type lgd:Place
    DataSet<Tuple2<Integer, Integer>> result = Stats.countMissingGeoAndTypeProperties(graphPath, true, env);

    result.map(value -> {
          assertEquals(12, value.f0.intValue()); // geo count
          assertEquals(12, value.f1.intValue()); // type count
          return value;
        })
        .returns(new TypeHint<Tuple2<Integer, Integer>>() {})
        .collect();
  }
}
