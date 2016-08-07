package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeGroupByTest {
  private static final Logger LOG = Logger.getLogger(TypeGroupByTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  /**
   * test if no type vertices are handled correctly
   */
  private static final String TGB_EQUAL_SIM_NO_TYPE_LOW_CCID = "g[" +
      "(v1 {ccId = 1L, compType = \"" + Constants.NO_TYPE + "\", hashCc = -4832605177143139923L})" +
      "(v2 {ccId = 1L, compType = \"" + Constants.NO_TYPE + "\", hashCc = 6500562624977345488L})" +
      "(v3 {ccId = 1L, compType = \"AdministrativeRegion\", hashCc = -8401086609692859185L})]" +
      "(v1)-[e1:sameAs {aggSimValue = 0.9428090453147888D}]->(v2)" +
      "(v1)-[e2:sameAs {aggSimValue = 0.9428090453147888D}]->(v3)";

  /**
   * test if lowest cc is resulting cc for all vertices (no type at all)
   */
  private static final String NO_TYPE_STRING = "g[" +
      "(v1 {label = \"a\", ccId = 1, typeIntern = \"" + Constants.NO_TYPE + "\"})" +
      "(v2 {label = \"b\", ccId = 1, typeIntern = \"" + Constants.NO_TYPE + "\"})" +
      "(v3 {label = \"c\", ccId = 1, typeIntern = \"" + Constants.NO_TYPE + "\"})]" +
      "(v1)-[e1:sameAs {aggSimValue = 0.9D}]->(v2)" +
      "(v1)-[e2:sameAs {aggSimValue = 0.9D}]->(v3)";

  private static final String TGB_SIMPLE = "g[" +
      "(v1 {ccId = 1, label = \"a\", typeIntern = \"" + Constants.NO_TYPE + "\"})" +
      "(v2 {ccId = 1, label = \"b\", typeIntern = \"Mountain\"})" +
      "(v3 {ccId = 1, label = \"c\", typeIntern = \"Settlement\"})" +
      "(v4 {ccId = 1, label = \"d\", typeIntern = \"Settlement\"})" +
      "(v5 {ccId = 2, label = \"e\", typeIntern = \"Settlement\"})" +
      "(v6 {ccId = 2, label = \"f\", typeIntern = \"Settlement\"})]" +
      "(v5)-[e4:sameAs {aggSimValue = .9D}]->(v6)" +
      "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
      "(v1)-[e2:sameAs {aggSimValue = .4D}]->(v3)" +
      "(v1)-[e3:sameAs {aggSimValue = .7D}]->(v4)";

  private static final String TGB_TRIPLE_UNKNOWN = "g[" +
      "(v1 {compType = \"Settlement\", hashCc = 12L})" +
      "(v2 {compType = \"" + Constants.NO_TYPE + "\", hashCc = 21L})" +
      "(v3 {compType = \"" + Constants.NO_TYPE + "\", hashCc = 33L})" +
      "(v4 {compType = \"" + Constants.NO_TYPE + "\", hashCc = 42L})" +
      "(v5 {compType = \"School\", hashCc = 51L})]" +
      "(v1)-[e1:sameAs {aggSimValue = .9D}]->(v2)" +
      "(v2)-[e2:sameAs {aggSimValue = .6D}]->(v3)" +
      "(v3)-[e3:sameAs {aggSimValue = .7D}]->(v4)" +
      "(v4)-[e4:sameAs {aggSimValue = .4D}]->(v5)";

  /**
   * Error occured only sometimes, therefore 5 graphs are computed and asserted.
   * @throws Exception
   */
  @Test
  public void tgbEqualSimNoTypeOnLowCcIdVertexTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(TGB_EQUAL_SIM_NO_TYPE_LOW_CCID);
    Graph<Long, ObjectMap, ObjectMap> graph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    graph = TypeGroupBy.execute(graph, Constants.DEFAULT_VALUE, 100, env, null);

    for (int i=0; i < 5; i++) {
      assertEquals(0, graph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
    }
  }

  /**
   * Error occured only sometimes, therefore 5 graphs are computed and asserted.
   * @throws Exception
   */
  @Test
  public void newTgbTest() throws Exception {
    String graphPath = TypeGroupByTest.class
        .getResource("/data/tgb/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    graph = TypeGroupBy.execute(graph, Constants.DEFAULT_VALUE, 100, env, null);

//    for (Vertex<Long, ObjectMap> vertex : graph.getVertices().collect()) {
//      if (vertex.getId() == 1375705L) {
//        assertEquals(4255728678492166934L, vertex.getValue().getHashCcId().longValue());
//      } else if (vertex.getId() == 617158L) {
//        assertEquals(4255728678492166934L, vertex.getValue().getHashCcId().longValue());
//      } else if (vertex.getId() == 617159L) {
//        assertEquals(4255728678492166934L, vertex.getValue().getHashCcId().longValue());
//      } else if (vertex.getId() == 1022884L) {
//        assertEquals(4255728678492166934L, vertex.getValue().getHashCcId().longValue());
////        assertEquals(8953605914864517116L, vertex.getValue().getHashCcId().longValue());
//      }
//    }

    graph.getVertices().print();
  }

  /**
   * No type at all for all vertices, get lowest cc id for all vertices in result
   */
  @Test
  public void noTypeTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(NO_TYPE_STRING);
    Graph<Long, ObjectMap, ObjectMap> graph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    graph = TypeGroupBy.execute(graph, Constants.DEFAULT_VALUE, 100, env, null);

    graph.getVertices().print();
//    for (int i=0; i < 5; i++) {
//      assertEquals(0, graph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
//    }
  }

  @Test
  public void typeGroupByTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(TGB_SIMPLE);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    firstGraph = TypeGroupBy.execute(firstGraph, Constants.DEFAULT_VALUE, 100, env, null);

    for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
      LOG.info(vertex.toString());
//      ObjectMap value = vertex.getValue();
//      if (value.getLabel().equals("a") || value.getLabel().equals("b")) {
//        assertEquals(-67319120785073684L, value.getHashCcId().longValue());
//      }
//      if (value.getLabel().equals("c") || value.getLabel().equals("d")) {
//        assertEquals(8342996591408486653L, value.getHashCcId().longValue());
//      }
    }

    // old todo
//    GDLHandler secondHandler = new GDLHandler.Builder().buildFromString(TGB_TRIPLE_UNKNOWN);
//    Graph<Long, ObjectMap, ObjectMap> secondGraph = MappingAnalysisExampleTest.createTestGraph(secondHandler);
//
//    secondGraph = TypeGroupBy.execute(secondGraph, Constants.DEFAULT_VALUE, 100, env, null);
//
//    for (Vertex<Long, ObjectMap> vertex : secondGraph.getVertices().collect()) {
//      ObjectMap value = vertex.getValue();
//      if (vertex.getId() == 5) {
//        assertTrue(value.get(Constants.COMP_TYPE).equals("School"));
//        assertEquals(value.get(Constants.HASH_CC), 51L);
//      } else {
//        assertEquals(value.get(Constants.HASH_CC), 12L);
//        assertTrue((value.containsKey(Constants.TMP_TYPE) && value.get(Constants.TMP_TYPE).equals("Settlement"))
//            || value.get(Constants.COMP_TYPE).equals("Settlement"));
//      }
//    }
  }

  private static class SpecificCcIdFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
    @Override
    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
      return (long) vertex.getValue().get(Constants.HASH_CC) != -8401086609692859185L;
    }
  }
}