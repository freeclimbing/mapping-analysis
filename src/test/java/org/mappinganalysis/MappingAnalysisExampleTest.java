package org.mappinganalysis;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simsort.SimSort;
import org.mappinganalysis.model.functions.typegroupby.TypeGroupBy;
import org.mappinganalysis.utils.Stats;
import org.mappinganalysis.utils.Utils;
import org.s1ck.gdl.GDLHandler;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * basic test class
 */
public class MappingAnalysisExampleTest {
  private static final Logger LOG = Logger.getLogger(MappingAnalysisExample.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String TGB_EQUAL_SIM_NO_TYPE_LOW_CCID = "g[" +
      "(v1 {compType = \"no_type_available\", hashCc = -4832605177143139923L})" +
      "(v2 {compType = \"no_type_available\", hashCc = 6500562624977345488L})" +
      "(v3 {compType = \"AdministrativeRegion\", hashCc = -8401086609692859185L})]" +
      "(v1)-[e1:sameAs {aggSimValue = 0.9428090453147888D}]->(v2)" +
      "(v1)-[e2:sameAs {aggSimValue = 0.9428090453147888D}]->(v3)";

  private static final String TGB_SIMPLE = "g[" +
      "(v1 {compType = \"no_type_available\", hashCc = 12L})" +
      "(v2 {compType = \"Mountain\", hashCc = 23L})" +
      "(v3 {compType = \"Settlement\", hashCc = 42L})" +
      "(v4 {compType = \"Settlement\", hashCc = 42L})]" +
      "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
      "(v1)-[e2:sameAs {aggSimValue = .4D}]->(v3)" +
      "(v1)-[e3:sameAs {aggSimValue = .7D}]->(v4)";

  private static final String SORT_SIMPLE = "g[" +
      "(v1 {typeIntern = \"Settlement\", label = \"bajaur\", lat = 34.683333D, lon = 71.5D, hashCc = 23L})" +
      "(v2 {typeIntern = \"Settlement\", label = \"Bajaur Agency\", lat = 34.6833D, lon = 71.5D, hashCc = 23L})" +
      "(v3 {typeIntern = \"AdministrativeRegion\", lat = 34.8333333D, lon = 71.5D, hashCc = 23L})" +
      "(v4 {label = \"Bajaur (Pakistan)\", lat = 34.8333D, lon = 71.5D, hashCc = 23L})]" +
      "(v4)-[e1:sameAs]->(v1)" +
      "(v4)-[e2:sameAs]->(v2)" +
      "(v3)-[e3:sameAs]->(v3)";

  private static final String TGB_TRIPLE_UNKNOWN = "g[" +
      "(v1 {compType = \"Settlement\", hashCc = 12L})" +
      "(v2 {compType = \"no_type_available\", hashCc = 21L})" +
      "(v3 {compType = \"no_type_available\", hashCc = 33L})" +
      "(v4 {compType = \"no_type_available\", hashCc = 42L})" +
      "(v5 {compType = \"School\", hashCc = 51L})]" +
      "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
      "(v2)-[e2:sameAs {aggSimValue = .6D}]->(v3)" +
      "(v3)-[e3:sameAs {aggSimValue = .7D}]->(v4)" +
      "(v4)-[e4:sameAs {aggSimValue = .4D}]->(v5)";

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
    Utils.PRE_CLUSTER_STRATEGY = Utils.CMD_COMBINED;
    Utils.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_SIMPLE);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, env);

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
    Utils.PRE_CLUSTER_STRATEGY = Utils.CMD_COMBINED;
    Utils.IGNORE_MISSING_PROPERTIES = true;
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_CANAIMA);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = createTestGraph(firstHandler);

    firstGraph = SimSort.prepare(firstGraph, env);

//    Stats.writeCcToLog(firstGraph, Lists.newArrayList(284L), Utils.CC_ID);
//    Stats.writeEdgesToLog(firstGraph, Lists.newArrayList(284L));
//    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
//      LOG.info(vertex);
//    }
//    for (Edge<Long, ObjectMap> edge : firstGraph.getEdges().collect()) {
//      LOG.info(edge);
//    }


    double minClusterSim = 0.75D;
    firstGraph = SimSort.execute(firstGraph, 200, minClusterSim);

//    Stats.writeCcToLog(exGraph, Lists.newArrayList(284L), Utils.CC_ID);
//    Stats.writeEdgesToLog(exGraph, Lists.newArrayList(284L));
    for (int i = 0; i < 1; i++) {
      for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
//        if (vertex.getId() == 3L || vertex.getId() == 4L) {
          LOG.info(vertex);
//          assertTrue(Doubles.compare((double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE), -2D) == 0);
//        }
      }
    }
//    fos (Edge<Long, ObjectMap> edge : exGraph.getEdges().collect()) {
//      LOG.info(edge);
//    }
  }

  /**
   * Error occured only sometimes, therefore 5 graphs are computed and asserted.
   * @throws Exception
   */
  @Test
  public void tgbEqualSimNoTypeOnLowCcIdVertexTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(TGB_EQUAL_SIM_NO_TYPE_LOW_CCID);
    Graph<Long, ObjectMap, ObjectMap> graph = createTestGraph(firstHandler);

    Graph<Long, ObjectMap, ObjectMap>firstGraph = new TypeGroupBy().execute(graph, 100);
    Graph<Long, ObjectMap, ObjectMap> secondGraph = new TypeGroupBy().execute(graph, 100);
    Graph<Long, ObjectMap, ObjectMap> thirdGraph = new TypeGroupBy().execute(graph, 100);
    Graph<Long, ObjectMap, ObjectMap> fourthGraph = new TypeGroupBy().execute(graph, 100);
    Graph<Long, ObjectMap, ObjectMap> fifthGraph = new TypeGroupBy().execute(graph, 100);

    assertEquals(0, firstGraph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
    assertEquals(0, secondGraph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
    assertEquals(0, thirdGraph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
    assertEquals(0, fourthGraph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
    assertEquals(0, fifthGraph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
  }

  @Test
  public void typeGroupByTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(TGB_SIMPLE);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = createTestGraph(firstHandler);

    firstGraph = new TypeGroupBy().execute(firstGraph, 100);

    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
      ObjectMap value = vertex.getValue();
      if (vertex.getId() == 1 || vertex.getId() == 2) {
        assertTrue((value.containsKey(Utils.TMP_TYPE) && value.get(Utils.TMP_TYPE).equals("Mountain"))
            || value.get(Utils.COMP_TYPE).equals("Mountain"));
        assertEquals(value.get(Utils.HASH_CC), 23L);
      } else {
        assertEquals(value.get(Utils.HASH_CC), 42L);
        assertTrue(value.get(Utils.COMP_TYPE).equals("Settlement"));
      }
    }

//    LOG.info("#### Second example ###");
    GDLHandler secondHandler = new GDLHandler.Builder().buildFromString(TGB_TRIPLE_UNKNOWN);
    Graph<Long, ObjectMap, ObjectMap> secondGraph = createTestGraph(secondHandler);

    secondGraph = new TypeGroupBy().execute(secondGraph, 100);

    for (Vertex<Long, ObjectMap> vertex : secondGraph.getVertices().collect()) {
      ObjectMap value = vertex.getValue();
      if (vertex.getId() == 5) {
        assertTrue(value.get(Utils.COMP_TYPE).equals("School"));
        assertEquals(value.get(Utils.HASH_CC), 51L);
      } else {
        assertEquals(value.get(Utils.HASH_CC), 12L);
        assertTrue((value.containsKey(Utils.TMP_TYPE) && value.get(Utils.TMP_TYPE).equals("Settlement"))
        || value.get(Utils.COMP_TYPE).equals("Settlement"));
      }
    }
  }

  private Graph<Long, ObjectMap, ObjectMap> createTestGraph(GDLHandler handler) {
    List<Edge<Long, ObjectMap>> edgeList = Lists.newArrayList();
    List<Vertex<Long, ObjectMap>> vertexList = Lists.newArrayList();

    // create Gelly edges and vertices -> graph
    for (org.s1ck.gdl.model.Vertex v : handler.getVertices()) {
      vertexList.add(new Vertex<>(v.getId(), new ObjectMap(v.getProperties())));
    }
    for (org.s1ck.gdl.model.Edge edge : handler.getEdges()) {
      edgeList.add(new Edge<>(edge.getSourceVertexId(),
          edge.getTargetVertexId(),
          new ObjectMap(edge.getProperties())));
    }

    return Graph.fromCollection(vertexList, edgeList, env);
  }

  private static class SpecificCcIdFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
    @Override
    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
      return (long) vertex.getValue().get(Utils.HASH_CC) != -8401086609692859185L;
    }
  }
}