package org.mappinganalysis.model.functions.simsort;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CcIdKeySelector;
import org.mappinganalysis.model.functions.HashCcIdKeySelector;
import org.mappinganalysis.utils.Utils;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.*;

public class SimSortTest {
  private static final Logger LOG = Logger.getLogger(SimSortTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String SORT_SIMPLE = "g[" +
      "(v1 {typeIntern = \"Settlement\", label = \"bajaur\", lat = 34.683333D, lon = 71.5D, hashCc = 23L})" +
      "(v2 {typeIntern = \"Settlement\", label = \"Bajaur Agency\", lat = 34.6833D, lon = 71.5D, hashCc = 23L})" +
      "(v3 {typeIntern = \"AdministrativeRegion\", lat = 34.8333333D, lon = 71.5D, hashCc = 23L})" +
      "(v4 {label = \"Bajaur (Pakistan)\", lat = 34.8333D, lon = 71.5D, hashCc = 23L})]" +
      "(v4)-[e1:sameAs]->(v1)" +
      "(v4)-[e2:sameAs]->(v2)" +
      "(v3)-[e3:sameAs]->(v3)";

  private static final String SORT_CANAIMA = "g[" +
      "(v1 {typeIntern = \"no_type_available\", label = \"Canaima (Venezuela)\", lat = 10.6311D, lon = -63.1917D, ccId = 284L})" +
      "(v2 {typeIntern = \"Settlement\", label = \"Canaima\", lat = 10.62809D, lon = -63.19147D, ccId = 284L})" +
      "(v3 {typeIntern = \"Park\", label = \"canaima national park\", lat = 6.166667D, lon = -62.5D, ccId = 284L})" +
      "(v4 {typeIntern = \"Park\", label = \"Canaima National Park\", lat = 6.16667D, lon = -62.5D, ccId = 284L})]" +
      "(v1)-[e1:sameAs]->(v2)" +
      "(v1)-[e2:sameAs]->(v3)" +
      "(v1)-[e3:sameAs]->(v4)";

  @Test
  public void simSortTest() throws Exception {
    Utils.PRE_CLUSTER_STRATEGY = Utils.DEFAULT_VALUE;
    Utils.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_SIMPLE);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, new HashCcIdKeySelector(), env);

    double minClusterSim = 0.75D;
    firstGraph = SimSort.execute(firstGraph, 200, minClusterSim);

    // TODO Test
    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
      LOG.info(vertex);
    }
    for (Edge<Long, ObjectMap> edge : firstGraph.getEdges().collect()) {
      LOG.info(edge);
    }
  }

  /**
   * Error occurs every 1-20(?) runs...
   * @throws Exception
   */
  @Test
  public void simSortErrorTest() throws Exception {
    Utils.PRE_CLUSTER_STRATEGY = Utils.DEFAULT_VALUE;
    Utils.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_CANAIMA);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, new CcIdKeySelector(), env);
    double minClusterSim = 0.75D;
    firstGraph = SimSort.execute(firstGraph, 200, minClusterSim);

    for (int i = 0; i < 20; i++) {
      for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
        if (vertex.getId() == 1L || vertex.getId() == 2L) {
          assertTrue(vertex.getValue().containsKey(Utils.VERTEX_STATUS));
        }
      }
    }
  }

  // TODO
  // geo dataset test? 1079
//  double minClusterSim = 0.75D;
//  simSortGraph = simSortGraph.filterOnVertices(new FilterFunction<Vertex<Long, ObjectMap>>() {
//    @Override
//    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
//      return vertex.getValue().containsKey(Utils.VERTEX_STATUS);
//    }
//  });
//  for (int i = 0; i < 20; i++) {
//    LOG.info("vertex_status_false  " + simSortGraph.getVertices().count());
//
//  }


  // TODO needed?
  //        LOG.info("#####");
//        LOG.info("##### 1. part: ");
//
//        for (Vertex<Long, ObjectMap> vertex : simSortGraph.getVertices().sortPartition(0, Order.ASCENDING).first(500).collect()) {
//          LOG.info(vertex);
//        }
//        LOG.info("#####");
//        LOG.info("##### 2. part");
//        for (Vertex<Long, ObjectMap> vertex : simSortGraph2.getVertices().sortPartition(0, Order.ASCENDING).first(500).collect()) {
//          LOG.info(vertex);
//        }
//        LOG.info("#####");
//        LOG.info("#####");

//        List<Long> ccList = Lists.newArrayList(284L);
//        LOG.info("### pre verts: ");
//        Stats.writeCcToLog(graph, ccList, Utils.CC_ID);
//        LOG.info("### verts: ");
//        Stats.writeCcToLog(simSortGraph, ccList, Utils.CC_ID);
}