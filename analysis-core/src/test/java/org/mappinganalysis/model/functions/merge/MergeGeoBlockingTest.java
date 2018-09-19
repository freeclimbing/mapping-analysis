package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.config.IncrementalConfig;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class MergeGeoBlockingTest {
  private static ExecutionEnvironment env;
  private static final Logger LOG = Logger.getLogger(MergeGeoBlockingTest.class);

  /**
   * Settlements test for csimq paper with input from Alieh.
   */
  @Test
  public void csimqSettlementTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    IncrementalConfig config = new IncrementalConfig(DataDomain.GEOGRAPHY, env);
    config.setMetric(Constants.COSINE_TRIGRAM);

    String graphPath =
        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/nentwig/settlement-benchmark/csimq/";
    List<String> sourceList = Lists.newArrayList(
        "5_0.75/"
//        ,
//        "6_0.8/", "7_0.85/",
// "8_0.9/"
    );
    for (String dataset : sourceList) {
      String pmPath = graphPath.concat(dataset);
      LogicalGraph logicalGraph = Utils
          .getGradoopGraph(pmPath, env);
      Graph<Long, ObjectMap, NullValue> inputGraph = Utils
          .getInputGraph(logicalGraph, Constants.GEO, env);

//      LOG.info("inEdges: " + inputGraph.getEdgeIds().count());
      Graph<Long, ObjectMap, ObjectMap> graph = inputGraph
          .run(new DefaultPreprocessing(config));

//      for (int simFor = 5; simFor <= 70; simFor += 5) {
//        double simThreshold = (double) simFor / 100;
      double simThreshold = 0.4; // TODO
      config.setSimSortSimilarity(simThreshold);

    /*
       representative creation
     */
      DataSet<Vertex<Long, ObjectMap>> representatives = graph
          .run(new SimSort(config))
          .getVertices()
          .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.GEOGRAPHY));

      /*
        tmp solution
       */
//      String outPath = "/home/markus/repos/mapping-analysis/analysis-core/target/test-classes/data/settlement-benchmark/csimq/";
//      String reprOut = outPath.concat("/output/");
//      new JSONDataSink(reprOut, "repr")
//          .writeVertices(representatives);
//
//      env.execute();

//        printQuality(dataset, 0.0, simThreshold, representatives, Constants.EMPTY_STRING, 4);
//      }


//      // merge
//      DataSet<Vertex<Long, ObjectMap>> diskRepresentatives =
//          new org.mappinganalysis.io.impl.json.JSONDataSource(
//              reprOut.concat("output/repr/"), true, env)
//              .getVertices();
//
////      for (int mergeFor = 50; mergeFor <= 95; mergeFor += 5) {
//        double mergeThreshold = 0.8;
////            (double) mergeFor / 100;
//
//        DataSet<Vertex<Long, ObjectMap>> merged = diskRepresentatives
//            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY))
//            .runOperation(new MergeExecution(
//                DataDomain.GEOGRAPHY,
//                config.getMetric(),
//                mergeThreshold,
//                4,
//                env));

        Config properties = new Config(DataDomain.GEOGRAPHY, env);
        properties.setProperty(Constants.DATASET, dataset);
//        properties.put(Constants.MERGE_THRESHOLD, mergeThreshold);
        properties.put(Constants.SIMSORT_THRESHOLD, simThreshold);
        properties.put(Constants.SOURCE_COUNT_LABEL, 4);

        QualityUtils.printGeoQuality(representatives, properties);
//      }
    }
  }

  /**
   * Check (especially) rejoin single vertices from SimSort:
   * 3 vertices where 2 are similar, get clustered
   * 2 vertices are dissimilar, they should be still in the result (as single vertices)
   * 2 vertices without oldHashCc
   */
  @Test
  public void testInit() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String filePath = MergeGeoBlockingTest.class
        .getResource("/data/representative/mergeInit/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = new JSONDataSource(filePath, true, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.GEOGRAPHY));

    int count = 0;
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      ++count;
      if (vertex.getId() == 395207L) {
//        System.out.println(vertex.toString());
//        System.out.println(vertex.getValue().getVerticesList());
        assertTrue(vertex.getValue().getVerticesList().contains(395207L)
            && vertex.getValue().getVerticesList().contains(513732L));
      } else {
        assertEquals(Sets.newHashSet(), vertex.getValue().getVerticesList());
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

    String graphPath = MergeGeoBlockingTest.class
        .getResource("/data/representative/mergeExec/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
        .getVertices()
        .runOperation(new MergeExecution(DataDomain.GEOGRAPHY,
            Constants.COSINE_TRIGRAM,
            0.5,
            5,
            env));

    // at some time, we had no(t always) reproducible results, here,
    // we check if the result is the same for multiple runs
    for (int i = 0; i < 4; i++) {
//      LOG.info("Run: " + i);
      for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
        if (vertex.getId() == 23L ) {
          assertTrue(vertex.getValue().getVerticesList().contains(23L)
              || vertex.getValue().getVerticesList().contains(42L));
        } else if (vertex.getId() == 395207L) {
          assertTrue(vertex.getValue().getVerticesList().contains(513732L)
              || vertex.getValue().getVerticesList().contains(395207L)
              || vertex.getValue().getVerticesList().contains(1010272L));
        } else if (vertex.getId() == 1L) {
          assertTrue(vertex.getValue().getVerticesList().contains(1L)
              || vertex.getValue().getVerticesList().contains(3L));
        } else if (vertex.getId() == 2L) {
          assertTrue(vertex.getValue().getVerticesList().contains(2L));
        } else if (vertex.getId() == 60190L){
          assertTrue(vertex.getValue().getVerticesList().contains(60191L)
              || vertex.getValue().getVerticesList().contains(252016L)
              || vertex.getValue().getVerticesList().contains(1268005L)
              || vertex.getValue().getVerticesList().contains(60190L));
        } else {
          assert false;
        }
      }
    }
  }

  @Test
  // weimar + weimar republic, lake louise
  public void testExecuteNoMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String graphPath = MergeGeoBlockingTest.class
        .getResource("/data/representative/mergeExec2/").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices = new JSONDataSource(graphPath, true, env)
        .getVertices()
        .runOperation(new MergeExecution(DataDomain.GEOGRAPHY,
            Constants.COSINE_TRIGRAM,
            0.5,
            5,
            env));

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

    String fail = Utils.getFinalValue(map);
    assertNull(fail);

    map.put("Leipzig, Sachsen", 1);
    map.put("Leipzig Saxonia Germany", 1);
    map.put("Leipzig (Sachsen)", 1);

    String finalValue = Utils.getFinalValue(map);
    assertTrue("Leipzig Saxonia Germany".equals(finalValue));

    map.put("Leipzig", 3);
    map.put("Lipsia Test", 2);
    finalValue = Utils.getFinalValue(map);

    assertTrue("Leipzig".equals(finalValue));
  }
}