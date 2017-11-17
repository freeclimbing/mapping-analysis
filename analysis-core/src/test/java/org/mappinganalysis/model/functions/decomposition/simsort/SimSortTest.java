package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.functions.MapFunction;
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

  /**
   * mystic example: simsort + representative
   * note: edge similarities are recomputed within test
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

    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
        .map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
//            System.out.println(vertex.toString());
            vertex.getValue().getDataSource();
//            System.out.println(vertex.toString() + "\n");
            return vertex;
          }
        });

    DataSet<Vertex<Long, ObjectMap>> representatives = vertices//graph.getVertices()
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