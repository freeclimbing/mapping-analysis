package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.decomposition.Clustering;
import org.mappinganalysis.model.functions.preprocessing.utils.ComponentSourceTuple;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

import static org.junit.Assert.*;

public class PreprocessingTest {
  private static final Logger LOG = Logger.getLogger(PreprocessingTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void typeMapperTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/typeMapping/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();
    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .mapVertices(new InternalTypeMapFunction())
        .getVertices();

    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      Set<String> types = vertex.getValue().getTypesIntern();
      if (vertex.getId() == 1L) {
        assertTrue(types.iterator().next()
            .equals(Constants.S));
      }
      else if (vertex.getId() == 2L || vertex.getId() == 3L) {
        assertTrue(types.iterator().next()
            .equals(Constants.NO_TYPE));
      }
      else if (vertex.getId() == 4L) {
        assertTrue(types.contains(Constants.S) && types.contains(Constants.M));
      }
      else if (vertex.getId() == 5L) {
        assertTrue(types.contains(Constants.S) && types.contains(Constants.AR));
      }
      else {
        assertFalse(true);
      }
    }
  }

  @Test
  public void compSourceTupleTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();

    DataSet<ComponentSourceTuple> resultTuples = Preprocessing
        .getComponentSourceTuples(graph.getVertices(), null);

    for (ComponentSourceTuple result : resultTuples.collect()) {
      assertEquals(60190L, result.getCcId().longValue());
      assertEquals(5, AbstractionUtils.getSourceCount(result).intValue());
      Set<String> sources = result.getSources();
      for (String source : sources) {
        assertTrue(source.equals(Constants.DBP_NS)
        || source.equals(Constants.FB_NS)
        || source.equals(Constants.NYT_NS)
        || source.equals(Constants.GN_NS)
        || source.equals(Constants.LGD_NS));
      }
    }
  }

  @Test
  // 819;label;Łęgowo;string
  // sometimes the following line is created
  // ����gowo
  // todo fix dont write to target folder
  public void writeToDiskEncodingTest() throws Exception {
        String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();


    // write to disk works like suspected with UTF8
    Utils.writeVerticesToJSONFile(graph.getVertices(), graphPath, true);

    graph.getVertices().print();
  }

  /**
   * Clustering link filter
   * @throws Exception
   */
  @Test
  public void finalOneToManyTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();

    graph = Clustering.computeTransitiveClosureEdgeSimilarities(graph, env);

    assertEquals(21, graph.getEdgeIds().count());

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setStrategy(LinkFilterStrategy.CLUSTERING)
        .build();

    for (Vertex<Long, ObjectMap> vertex : graph.run(linkFilter).getVertices().collect()) {
      LOG.info(vertex.toString());
      assertTrue(vertex.getId() == 60191L
          || vertex.getId() == 252016L
          || vertex.getId() == 513732L
          || vertex.getId() == 60190L
          || vertex.getId() == 1268005L);
    }
  }

  /**
   * Check basic link filter, optionally delete isolated vertices - Gradina
   *
   * 7 vertices, delete 5
   * vertex lat/lon data is irrelevant, similarities are already computed in edges
   * TODO too much collect
   */
  @Test
  public void oneToManyTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/oneToMany/").getFile();
    Graph<Long, ObjectMap, ObjectMap> inputGraph = new JSONDataSource(graphPath, true, env).getGraph();

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(true)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

    Graph<Long, ObjectMap, ObjectMap> resultDeleteVerticesGraph = inputGraph.run(linkFilter);

    // test - one edge, two vertices
    for (Tuple2<Long, Long> edge : resultDeleteVerticesGraph.getEdgeIds().collect()) {
      assertEquals(2642L, edge.f0.longValue());
      assertEquals(46584L, edge.f1.longValue());
    }
    for (Long vertex : resultDeleteVerticesGraph.getVertexIds().collect()) {
      assertTrue(vertex == 2642L || vertex == 46584L);
    }

//    Graph<Long, ObjectMap, ObjectMap> graph =
//        new JSONDataSource(graphPath, true, env).getGraph()
//            .run(linkFilter)
//            .filterOnEdges(edge -> {
//              if (edge.getSource() == 617158L) {
//                // 3 edges to lgd get reduced to the best option
//                assertEquals(617159L, edge.getTarget().longValue());
//                return true;
//              } else if (edge.getSource() == 1022884L) {
//                // 2 edges to geonames to best option
//                assertEquals(1375705L, edge.getTarget().longValue());
//                return true;
//              } else {
//                return false;
//              }
//            });

    linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(false)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

    Graph<Long, ObjectMap, ObjectMap> resultNoDeleteVerticesGraph = inputGraph.run(linkFilter);

    // test - same edge, but all vertices still in graph
    for (Tuple2<Long, Long> edge : resultNoDeleteVerticesGraph.getEdgeIds().collect()) {
      assertEquals(2642L, edge.f0.longValue());
      assertEquals(46584L, edge.f1.longValue());
    }
    assertEquals(7, resultNoDeleteVerticesGraph.getVertices().count());
  }

  /**
   * EqualDataSourceLinkRemover not tested, but tested separately
   * @throws Exception
   */
  @Test
  public void defaultPreprocessingTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/defaultPreprocessing/").getFile();
    Graph<Long, ObjectMap, NullValue> inGraph = new JSONDataSource(graphPath, true, env)
        .getGraph(ObjectMap.class, NullValue.class);
    DefaultPreprocessing preprocessing = new DefaultPreprocessing(env);

    inGraph.run(preprocessing).getVertices().print();
  }

  /**
   * Link to vertex 1 is deleted
   * @throws Exception
   */
  @Test
  public void typeMisMatchCorrectionTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/defaultPreprocessing/").getFile();
    Graph<Long, ObjectMap, NullValue> graph = new JSONDataSource(graphPath, true, env)
        .getGraph(ObjectMap.class, NullValue.class);

    assertEquals(6, graph
        .run(new TypeMisMatchCorrection(env))
        .filterOnEdges(new FilterFunction<Edge<Long, NullValue>>() { // no lambda
          @Override
          public boolean filter(Edge<Long, NullValue> edge) throws Exception {
            assertTrue(edge.getSource() != 1L && edge.getTarget() != 1L);
            return true;
          }
        })
        .getEdgeIds()
        .count());
  }

  /**
   * 5 vertices, one gets deleted - Karlespitze
   */
  @Test
  public void deleteVerticesWithoutEdgesTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/deleteVerticesWithoutEdges/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();

    DataSet<Vertex<Long, ObjectMap>> result = graph.getVertices()
        .runOperation(new IsolatedVertexRemover<>(graph.getEdges()));

    assertEquals(4, result.count());
  }
}
