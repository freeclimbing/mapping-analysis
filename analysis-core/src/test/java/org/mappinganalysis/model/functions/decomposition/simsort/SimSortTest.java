package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;
import org.mappinganalysis.util.Constants;
import org.s1ck.gdl.GDLHandler;

import static org.junit.Assert.assertTrue;

public class SimSortTest {
  private static final Logger LOG = Logger.getLogger(SimSortTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String SORT_SIMPLE = "g[" +
//      "(v1 {ccId = 1L, typeIntern = \"Settlement\", label = \"bajaur\", lat = 34.683333D, lon = 71.5D, hashCc = 23L})" +
      "(v1 {ccId = 1L, typeIntern = \"Settlement\", label = \"zzzzz\", lat = 34.683333D, lon = 71.5D, hashCc = 23L})" +
      "(v2 {ccId = 1L, typeIntern = \"Settlement\", label = \"Bajaur Agency\", lat = 34.6833D, lon = 71.5D, hashCc = 23L})" +
      "(v3 {ccId = 1L, typeIntern = \"AdministrativeRegion\", lat = 34.8333333D, lon = 71.5D, hashCc = 23L})" +
      "(v4 {ccId = 1L, label = \"Bajaur (Pakistan)\", lat = 34.8333D, lon = 71.5D, hashCc = 23L})]" +
      "(v4)-[e1:sameAs {foo = \"bar\"}]->(v1)" +
      "(v4)-[e2:sameAs {foo = \"bar\"}]->(v2)" +
      "(v3)-[e3:sameAs {foo = \"bar\"}]->(v3)";

  private static final String SORT_CANAIMA = "g[" +
      "(v1 {typeIntern = \"no_type_available\", label = \"Canaima (Venezuela)\", lat = 10.6311D, lon = -63.1917D, ccId = 284L})" +
      "(v2 {typeIntern = \"Settlement\", label = \"Canaima\", lat = 10.62809D, lon = -63.19147D, ccId = 284L})" +
      "(v3 {typeIntern = \"Park\", label = \"canaima national park\", lat = 6.166667D, lon = -62.5D, ccId = 284L})" +
      "(v4 {typeIntern = \"Park\", label = \"Canaima National Park\", lat = 6.16667D, lon = -62.5D, ccId = 284L})]" +
      "(v1)-[e1:sameAs]->(v2)" +
      "(v1)-[e2:sameAs]->(v3)" +
      "(v1)-[e3:sameAs]->(v4)";

  /**
   * mystic example: simsort + representative
   * todo check sims
   */
  @Test
  // TODO write asserts
  public void simSortJSONTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    double minSimilarity = 0.8;

    String graphPath = SimSortTest.class.getResource("/data/simsort/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph()
            .run(new SimSort(DataDomain.GEOGRAPHY, minSimilarity, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = graph.getVertices()
        .runOperation(new RepresentativeCreator(DataDomain.GEOGRAPHY));

    for (Vertex<Long, ObjectMap> vertex : representatives.collect()) {
//      LOG.info(vertex.toString());
      if (vertex.getId() == 2757L) {
        assertTrue(vertex.getValue().getVerticesCount().equals(1));
      } else {
        assertTrue(vertex.getValue().getVerticesCount().equals(3));
      }
    }
  }

  /**
   * Error occurs every 1-20(?) runs... not anymore
   * @throws Exception
   */
  @Test
  public void simSortErrorTest() throws Exception {
    GDLHandler firstHandler = new GDLHandler.Builder().buildFromString(SORT_CANAIMA);
    Graph<Long, ObjectMap, ObjectMap> firstGraph = MappingAnalysisExampleTest.createTestGraph(firstHandler);
    double minSimilarity = 0.75D;

    firstGraph = firstGraph.run(new SimSort(true, minSimilarity, env));

    for (int i = 0; i < 20; i++) {
      for (Vertex<Long, ObjectMap> vertex : firstGraph.getVertices().collect()) {
        if (vertex.getId() == 1L || vertex.getId() == 2L) {
          assertTrue(vertex.getValue().containsKey(Constants.VERTEX_STATUS));
        }
      }
    }
  }
}