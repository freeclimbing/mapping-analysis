package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;


public class MergeTest {
  private static ExecutionEnvironment env;
  private static final Logger LOG = Logger.getLogger(MergeTest.class);

  /**
   * Check (especially) rejoin single vertices from SimSort:
   * - 3 vertices where 2 are similar, get clustered
   * - 2 vertices are dissimilar, they should be still in the result (as single vertices)
   * - 2 vertices without oldHashCc
   * @throws Exception
   */
  @Test
  public void testInit() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeInit/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        Utils.readVerticesFromJSONFile(graphPath, env, true);

    vertices = Merge.init(vertices, null);

    int count = 0;
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      LOG.info(vertex.toString());
      ++count;
      if (vertex.getId() == 395207L) {
        assertTrue(vertex.getValue().getVerticesList().contains(395207L)
            && vertex.getValue().getVerticesList().contains(513732L));
      } else {
        assertNull(vertex.getValue().getVerticesList());
      }
    }
    assertEquals(6, count);
  }

  @Test
  /**
   * Long Island, real data mixed with fake data.
   *
   * Note: one degree longitude reduces geo sim by ~50%
   *
   * 1, 2, 3: two have only one geo attribute
   */
  public void testExecuteMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

    vertices = Merge.execute(vertices, 5);

//    int i = 0;
//    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
//      assertFalse(vertex.getValue().getTypes(Constants.TYPE_INTERN).contains(Constants.NO_TYPE));
//      ++i;
//    }
//    assertEquals(3, i);

    vertices.print();
  }

  @Test
  // weimar + weimar republic, lake louise
  public void testExecuteNoMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec2/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

    vertices = Merge.execute(vertices, 5);

    assertEquals(4, vertices.count());
//    vertices.print();
  }

  @Test
  public void testAddBlockingLabel() throws Exception {
    String testLabel = "foobar";
    String blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("foo"));
    blockingLabel = Utils.getBlockingLabel(testLabel.substring(0, 1));
    assertTrue(blockingLabel.equals("f##"));
    testLabel = "+5";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("#5#"));

    testLabel = "Long Island, NY";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("lon"));

    testLabel = "N123";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("n12"));
    testLabel = "1ABC";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("1ab"));

    testLabel = "安市";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("###"));

    testLabel = "ﻚﻓﺭ ﺐﻬﻣ";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("###"));

    testLabel = "Pułaczów";
    blockingLabel = Utils.getBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("pu#"));
  }

  @Test
  public void testGetFinalValue() throws Exception {
    HashMap<String, Integer> map = Maps.newHashMap();
    map.put("Leipzig, Sachsen", 1);
    map.put("Leipzig Saxonia Germany", 1);
    map.put("Leipzig (Sachsen)", 1);

    String finalValue = Merge.getFinalValue(map, Constants.LABEL);
    assertTrue("Leipzig Saxonia Germany".equals(finalValue));

    map.put("Leipzig", 3);
    map.put("Lipsia Test", 2);
    finalValue = Merge.getFinalValue(map, Constants.LABEL);

    assertTrue("Leipzig".equals(finalValue));
  }
}