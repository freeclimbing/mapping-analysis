package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;

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
    setupLocalEnvironment();
    setupConstants();

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
   */
  public void testExecuteMerge() throws Exception {
    setupLocalEnvironment();
    setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

    vertices = Merge.execute(vertices, 5, null, env);

    int i = 0;
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      assertFalse(vertex.getValue().getTypes(Constants.TYPE_INTERN).contains(Constants.NO_TYPE));
      ++i;
    }
    assertEquals(3, i);

//    vertices.print();
    // result:
    // (42,({typeIntern=[Island], lon=-73.0662, label=Long Island (NY), clusteredVertices=[42], ontologies=[http://data.nytimes.com/], lat=40.8168}))
    //(395207,({typeIntern=[Island], lon=-73.1134, label=Long Island, lat=41.2182, ontologies=[http://linkedgeodata.org/, http://sws.geonames.org/, http://data.nytimes.com/], clusteredVertices=[395207, 513732, 1010272]}))
    //(23,({typeIntern=[Island], lon=-73.0662, label=Long Island, clusteredVertices=[252016, 1268005, 23, 60190, 60191], ontologies=[http://dbpedia.org/, http://linkedgeodata.org/, http://sws.geonames.org/, http://rdf.freebase.com/, http://data.nytimes.com/], lat=40.8168}))
  }

  @Test
  // weimar + weimar republic, lake louise
  public void testExecuteNoMerge() throws Exception {
    setupLocalEnvironment();
    setupConstants();

    String graphPath = MergeTest.class
        .getResource("/data/representative/mergeExec2/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = Utils.readFromJSONFile(graphPath, env, true)
        .getVertices();

    vertices = Merge.execute(vertices, 5, null, env);

    assertEquals(4, vertices.count());
//    vertices.print();
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

    input = "Long Island, NY";
    blockingLabel = Utils.getBlockingLabel(input);
    assertTrue(blockingLabel.equals("lon"));
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

  private void setupConstants() {
    Constants.MIN_CLUSTER_SIM = 0.5;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    Constants.MIN_LABEL_PRIORITY_SIM = 0.5;
    Constants.INPUT_DIR = "linklion";
    Constants.SOURCE_COUNT = 5;
  }

  private void setupLocalEnvironment() {
    Configuration conf = new Configuration();
    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 16384);
    env = new LocalEnvironment(conf);
    env.setParallelism(Runtime.getRuntime().availableProcessors());
    env.getConfig().disableSysoutLogging();
  }

}