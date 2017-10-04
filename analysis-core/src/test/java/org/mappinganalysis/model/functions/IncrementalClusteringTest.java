package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;

public class IncrementalClusteringTest {
  private static final Logger LOG = Logger.getLogger(org.mappinganalysis.model.functions.decomposition.simsort.SimSortTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void incTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.MINSIZE)
        .build();

    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/preprocessing/oneToMany").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph()
            .run(clustering);

    graph.getVertices().print();

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