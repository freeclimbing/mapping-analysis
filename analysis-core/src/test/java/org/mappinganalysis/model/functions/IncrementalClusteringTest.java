package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import static org.junit.Assert.assertEquals;

public class IncrementalClusteringTest {
  private static final Logger LOG = Logger.getLogger(org.mappinganalysis.model.functions.decomposition.simsort.SimSortTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void fixedTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED)
        .build();

    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph()
            .run(clustering);
  }

  @Test
  public void incCountDataSourceElementsTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.MINSIZE)
        .build();

    String graphPath = IncrementalClusteringTest.class
//        .getResource("/data/preprocessing/oneToMany").getFile();
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph()
            .run(clustering);

    for (Vertex<Long, ObjectMap> vertex : graph.getVertices().collect()) {
      ObjectMap properties = vertex.getValue();

      if (properties.getDataSource().equals(Constants.GN_NS)) {
        assertEquals(749L, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.NYT_NS)) {
        assertEquals(755, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.DBP_NS)) {
        assertEquals(774, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.FB_NS)) {
        assertEquals(776, properties.getDataSourceEntityCount().longValue());
      }
    }

//    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
//        .map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
//          @Override
//          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
//            System.out.println(vertex.toString());
//            vertex.getValue().getDataSource();
//            System.out.println(vertex.toString() + "\n");
//            return vertex;
//          }
//        });
//
//    DataSet<Vertex<Long, ObjectMap>> representatives = vertices//graph.getVertices()
//        .runOperation(new RepresentativeCreator(DataDomain.GEOGRAPHY));
//
//    for (Vertex<Long, ObjectMap> vertex : representatives.collect()) {
//      LOG.info(vertex.toString());
//      if (vertex.getId() == 2757L) {
//        assertTrue(vertex.getValue().getVerticesCount().equals(1));
//      } else {
//        assertTrue(vertex.getValue().getVerticesCount().equals(3));
//      }
//    }

  }
}