package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;

import static org.junit.Assert.assertTrue;

public class SimSortTest {
  private static final Logger LOG = Logger.getLogger(SimSortTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private static final String SORT_CANAIMA = "g[" +
      "(v1 {typeIntern = \"no_type_available\", label = \"Canaima (Venezuela)\", lat = 10.6311D, lon = -63.1917D, ccId = 284L})" +
      "(v2 {typeIntern = \"Settlement\", label = \"Canaima\", lat = 10.62809D, lon = -63.19147D, ccId = 284L})" +
      "(v3 {typeIntern = \"Park\", label = \"canaima national park\", lat = 6.166667D, lon = -62.5D, ccId = 284L})" +
      "(v4 {typeIntern = \"Park\", label = \"Canaima National Park\", lat = 6.16667D, lon = -62.5D, ccId = 284L})]" +
      "(v1)-[e1:sameAs {foo = \"bar\"}]->(v2)" +
      "(v1)-[e2:sameAs {foo = \"bar\"}]->(v3)" +
      "(v1)-[e3:sameAs {foo = \"bar\"}]->(v4)";

  /**
   * mystic example: simsort + representative
   * todo check sims
   */
  @Test
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
}