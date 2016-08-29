package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Vertex;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;

import static org.junit.Assert.*;


public class MergeTest {
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
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
    Constants.MIN_CLUSTER_SIM = 0.5;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    Constants.MIN_LABEL_PRIORITY_SIM = 0.5;

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeInit/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

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
  public void testExecute() throws Exception {
    Constants.MIN_CLUSTER_SIM = 0.5;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    Constants.MIN_LABEL_PRIORITY_SIM = 0.5;
    Constants.INPUT_DIR = "linklion";


    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

    vertices = Merge.execute(vertices, null);

    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      LOG.info(vertex.toString());
    }
  }

  @Test
  public void testAddBlockingLabel() throws Exception {
    String input = "foobar";
    String blockingLabel = Utils.getBlockingLabel(input);
    assertTrue(blockingLabel.equals("foo"));
    blockingLabel = Utils.getBlockingLabel(input.substring(0, 3));
    assertTrue(blockingLabel.equals("foo"));
    blockingLabel = Utils.getBlockingLabel(input.substring(0, 1));
    assertTrue(blockingLabel.equals("f##"));

    input = "5";
    blockingLabel = Utils.getBlockingLabel(input);
    assertTrue(blockingLabel.equals("###"));
    input = "5555";
    blockingLabel = Utils.getBlockingLabel(input);
    assertTrue(blockingLabel.equals("###"));
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