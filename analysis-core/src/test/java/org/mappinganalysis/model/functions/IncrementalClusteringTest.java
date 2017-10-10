package org.mappinganalysis.model.functions;

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
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.impl.Representative;
import org.mappinganalysis.model.impl.RepresentativeMap;
import org.mappinganalysis.util.Constants;

import static org.junit.Assert.assertEquals;

public class IncrementalClusteringTest {
  private static final Logger LOG = Logger.getLogger(org.mappinganalysis.model.functions.decomposition.simsort.SimSortTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  @Test
  public void fixedStrategyIncClusteringTest() throws Exception {
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
  public void createReprTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
          .getGraph();

    DataSet<Vertex<Long, ObjectMap>> output = graph.getVertices()
        .first(1)
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

    for (Vertex<Long, ObjectMap> representative : output.collect()) {
      LOG.info("result: " + representative.toString());
    }

    // todo preprocess data

    // todo check result
  }

  @Test
  public void customReprTest() throws Exception {
        String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
          .getGraph();

    DataSet<Representative> output = graph.getVertices()
        .first(1)
        .map(new MapFunction<Vertex<Long, ObjectMap>, Representative>() {
          @Override
          public Representative map(Vertex<Long, ObjectMap> value) throws Exception {
            LOG.info("####");
            Representative representative = new Representative(value, DataDomain.GEOGRAPHY);
            LOG.info("rep: " + representative.toString());

            RepresentativeMap props = representative.getValue();
            LOG.info(props.size());
//            props.setBlockingKey(BlockingStrategy.STANDARD_BLOCKING);
//            LOG.info(props.size());
            LOG.info("props: " + props);

            return representative;
          }
        });

        output.print();

  }

  @Test
  public void incCountDataSourceElementsTest() throws Exception {
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