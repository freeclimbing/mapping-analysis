package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeGroupByTest {
  private static final Logger LOG = Logger.getLogger(TypeGroupByTest.class);

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

//  @Test
//  public void geoDatasetTest() throws Exception {
//    Graph<Long, ObjectMap, ObjectMap> typeGroupByGraph
//        = new TypeGroupBy().execute(graph, 5);
//
//  }

  /**
   * Error occured only sometimes, therefore 5 graphs are computed and asserted.
   * @throws Exception
   */
  @Test
  public void tgbEqualSimNoTypeOnLowCcIdVertexTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(TGB_EQUAL_SIM_NO_TYPE_LOW_CCID);
    Graph<Long, ObjectMap, ObjectMap> graph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

    graph = new TypeGroupBy().execute(graph, 100);

    for (int i=0; i < 5; i++) {
      assertEquals(0, graph.filterOnVertices(new SpecificCcIdFilter()).getVertices().count());
    }
  }

  @Test
  public void typeGroupByTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(TGB_SIMPLE);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);

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

    GDLHandler secondHandler = new GDLHandler.Builder().buildFromString(TGB_TRIPLE_UNKNOWN);
    Graph<Long, ObjectMap, ObjectMap> secondGraph = MappingAnalysisExampleTest.createTestGraph(secondHandler);

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

  private static class SpecificCcIdFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
    @Override
    public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
      return (long) vertex.getValue().get(Utils.HASH_CC) != -8401086609692859185L;
    }
  }
}