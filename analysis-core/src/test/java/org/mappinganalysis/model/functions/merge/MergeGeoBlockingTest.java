package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;

import static org.junit.Assert.*;


public class MergeGeoBlockingTest {
  private static ExecutionEnvironment env;
  private static final Logger LOG = Logger.getLogger(MergeGeoBlockingTest.class);

  /**
   * Check (especially) rejoin single vertices from SimSort:
   * 3 vertices where 2 are similar, get clustered
   * 2 vertices are dissimilar, they should be still in the result (as single vertices)
   * 2 vertices without oldHashCc
   * @throws Exception
   */
  @Test
  public void testInit() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String filePath = MergeGeoBlockingTest.class
        .getResource("/data/representative/mergeInit/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = new JSONDataSource(filePath, true, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY));

//    vertices.print();

    int count = 0;
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
//      LOG.info(vertex.toString());
      ++count;
      if (vertex.getId() == 395207L) {
//        System.out.println(vertex.toString());
//        System.out.println(vertex.getValue().getVerticesList());
        assertTrue(vertex.getValue().getVerticesList().contains(395207L)
            && vertex.getValue().getVerticesList().contains(513732L));
      } else {
        assertNull(vertex.getValue().getVerticesList());
      }
    }
    assertEquals(6, count);
  }

  /**
   * Long Island, real data mixed with fake data.
   *
   * Note: one degree longitude reduces geo sim by ~50%
   *
   * 1, 2, 3: two have only one geo attribute
   */
  @Test
  public void testExecuteMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeGeoBlockingTest.class
        .getResource("/data/representative/mergeExec/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =

        new JSONDataSource(graphPath, true, env)
        .getVertices()
        .runOperation(new MergeExecution(DataDomain.GEOGRAPHY, 5, env));

    // at some time, we had no(t always) reproducible results, here,
    // we check if the result is the same for 10 runs
    // todo remove outer for loop later
    for (int i = 0; i < 4; i++) {
      LOG.info("Run: " + i);
      for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
//        LOG.info(vertex.toString());

        // add shading test

        if (vertex.getId() == 60191L) {
          assertTrue(vertex.getValue().getVerticesList().contains(60191L));
        } else if (vertex.getId() == 395207L) {
          assertTrue(vertex.getValue().getVerticesList().contains(513732L)
              || vertex.getValue().getVerticesList().contains(395207L)
              || vertex.getValue().getVerticesList().contains(1010272L));
        } else if (vertex.getId() == 1L) {
          assertTrue(vertex.getValue().getVerticesList().contains(1L));
        } else if (vertex.getId() == 2L) {
          assertTrue(vertex.getValue().getVerticesList().contains(2L)
              || vertex.getValue().getVerticesList().contains(3L));
        } else if (vertex.getId() == 23L){
          assertTrue(vertex.getValue().getVerticesList().contains(23L)
              || vertex.getValue().getVerticesList().contains(42L)
              || vertex.getValue().getVerticesList().contains(252016L)
              || vertex.getValue().getVerticesList().contains(1268005L)
              || vertex.getValue().getVerticesList().contains(60190L));
        } else {
          assert false;
        }

//        if (vertex.getId() == 60191L
//            && vertex.getValue().getVerticesList().size() == 1) {
//          LOG.info("60191L single");
//        } else if (vertex.getId() == 42L
//            && vertex.getValue().getVerticesList().size() == 1) {
//          LOG.info("42L single");
//        }
      }
    }
  }

  @Test
  // weimar + weimar republic, lake louise
  public void testExecuteNoMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();

    String graphPath = MergeGeoBlockingTest.class
        .getResource("/data/representative/mergeExec2/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = new JSONDataSource(graphPath, true, env)
        .getVertices()
        .runOperation(new MergeExecution(DataDomain.GEOGRAPHY, 5, env));

//    vertices.print();
    assertEquals(4, vertices.count());
  }

  @Test
  public void testAddBlockingLabel() throws Exception {
    String testLabel = "foobar";
    String blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("foo"));
    blockingLabel = Utils.getGeoBlockingLabel(testLabel.substring(0, 1));
    assertTrue(blockingLabel.equals("f##"));
    testLabel = "+5";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("#5#"));

    testLabel = "Long Island, NY";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("lon"));

    testLabel = "N123";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("n12"));
    testLabel = "1ABC";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("1ab"));

    testLabel = "安市";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("###"));

    testLabel = "ﻚﻓﺭ ﺐﻬﻣ";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("###"));

    testLabel = "Pułaczów";
    blockingLabel = Utils.getGeoBlockingLabel(testLabel);
    assertTrue(blockingLabel.equals("pu#"));
  }

  @Test
  public void testGetFinalValue() throws Exception {
    HashMap<String, Integer> map = Maps.newHashMap();
    map.put("Leipzig, Sachsen", 1);
    map.put("Leipzig Saxonia Germany", 1);
    map.put("Leipzig (Sachsen)", 1);

    String finalValue = Utils.getFinalValue(map, Constants.LABEL);
    assertTrue("Leipzig Saxonia Germany".equals(finalValue));

    map.put("Leipzig", 3);
    map.put("Lipsia Test", 2);
    finalValue = Utils.getFinalValue(map, Constants.LABEL);

    assertTrue("Leipzig".equals(finalValue));
  }
}