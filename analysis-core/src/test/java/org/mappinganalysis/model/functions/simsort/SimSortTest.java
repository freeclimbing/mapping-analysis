package org.mappinganalysis.model.functions.simsort;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  /**
   * Complete "small" dataset simsort test.
   * @throws Exception
   */
  @Test
  public void simSortJSONTest() throws Exception {
    Constants.PRE_CLUSTER_STRATEGY = Constants.DEFAULT_VALUE;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    Constants.MIN_SIMSORT_SIM = 0.5;

    String graphPath = SimSortTest.class.getResource("/data/simsort/").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    graph = SimSort.execute(graph, 100);
    graph = SimSort.excludeLowSimVertices(graph, env);
    graph = GraphUtils.applyLinkFilter(graph, env);
    // old default result: 7115
    //graph = Preprocessing.applyLinkFilterStrategy(graph, env, true);
    LOG.info(graph.getVertexIds().count());

    assertEquals(7533, graph.getVertexIds().count());
  }

  @Test
  public void simSortTest() throws Exception {
    Constants.PRE_CLUSTER_STRATEGY = Constants.DEFAULT_VALUE;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_SIMPLE);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, env, null);

    // ?? needed?
    Constants.MIN_CLUSTER_SIM = 0.75D;
    firstGraph = SimSort.execute(firstGraph, 100);

    // TODO Test
    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
      LOG.info(vertex);
    }
//    for (Edge<Long, ObjectMap> edge : firstGraph.getEdges().collect()) {
//      LOG.info(edge);
//    }
  }

  /**
   * Error occurs every 1-20(?) runs...
   * @throws Exception
   */
  @Test
  public void simSortErrorTest() throws Exception {
    Constants.PRE_CLUSTER_STRATEGY = Constants.DEFAULT_VALUE;
    Constants.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_CANAIMA);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, env, null);
    Constants.MIN_CLUSTER_SIM = 0.75D;
    firstGraph = SimSort.execute(firstGraph, 200);

    for (int i = 0; i < 20; i++) {
      for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
        if (vertex.getId() == 1L || vertex.getId() == 2L) {
          assertTrue(vertex.getValue().containsKey(Constants.VERTEX_STATUS));
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
}