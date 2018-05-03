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
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.Clustering;
import org.mappinganalysis.model.functions.preprocessing.utils.ComponentSourceTuple;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.Set;

import static org.junit.Assert.*;

public class PreprocessingTest {
  private static final Logger LOG = Logger.getLogger(PreprocessingTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void typeMapperTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/typeMapping/")
        .getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
        .getGraph();

    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .mapVertices(new InternalTypeMapFunction())
        .getVertices();

    // tests
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

  /**
   * Test aux method
   * csv reader
   */
  @Test
  public void compSourceTupleTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(graphPath, true, env).getGraph();

    DataSet<ComponentSourceTuple> resultTuples = CSVDataSource
        .getComponentSourceTuples(graph.getVertices(), null);

    // tests
    for (ComponentSourceTuple result : resultTuples.collect()) {
      assertEquals(60190L, result.getCcId().longValue());
      assertEquals(5, AbstractionUtils.getSourceCount(result));
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

//  @Test
//  // 819;label;Łęgowo;string
//  // sometimes the following line is created
//  // ����gowo
//  // todo fix dont write to target folder
//  public void writeToDiskEncodingTest() throws Exception {
//        String graphPath = PreprocessingTest.class
//        .getResource("/data/preprocessing/general/").getFile();
//    DataSet<Vertex<Long, ObjectMap>> vertices =
//        new JSONDataSource(graphPath, true, env).getVertices();
//
//    // TODO use discarding output format to check encoding?
//    // use somewhat like this vertexOutFile = outDir + "output/";
//    JSONDataSink dataSink = new JSONDataSink(graphPath);
//    // write to disk works like suspected with UTF8
//    dataSink.writeVertices(vertices);
//
//    vertices.print();
//  }

  /**
   * Clustering link filter
   */
  @Test
  public void finalOneToManyTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
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
//      LOG.info(vertex.toString());
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
   */
  @Test
  public void linkFilterVertexDeleteTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/oneToMany/").getFile();
    Graph<Long, ObjectMap, ObjectMap> inputGraph
        = new JSONDataSource(graphPath, true, env)
        .getGraph();

    assertEquals(9, inputGraph.getEdges().count());
    assertEquals(11, inputGraph.getVertices().count());

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(true) // <<---
        .setDataSources(Constants.GEO_SOURCES)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

    Graph<Long, ObjectMap, ObjectMap> resultDeleteVerticesGraph = inputGraph
        .run(linkFilter);

    // 5 vertices removed
    assertEquals(6, resultDeleteVerticesGraph.getVertices().count());

    for (Tuple2<Long, Long> edge : resultDeleteVerticesGraph.getEdgeIds().collect()) {
      if (edge.f0 == 4L) {
        assertEquals(5L, edge.f1.longValue());
      } else if (edge.f0 == 6L) {
        assertEquals(7L, edge.f1.longValue());
      } else if (edge.f0 == 2642L) {
        assertEquals(46584L, edge.f1.longValue());
      } else {
        assertFalse(true); // 6 edges removed
      }
    }
  }

  @Test
  public void linkFilterNoVertexDeleteTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/oneToMany/").getFile();
    Graph<Long, ObjectMap, ObjectMap> inputGraph
        = new JSONDataSource(graphPath, true, env)
        .getGraph();

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(false)
        .setDataSources(Constants.GEO_SOURCES)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

    Graph<Long, ObjectMap, ObjectMap> resultNoDeleteGraph = inputGraph
        .run(linkFilter);

    assertEquals(11, resultNoDeleteGraph.getVertices().count());
    assertEquals(3, resultNoDeleteGraph.getEdges().count());
  }

  @Test
  public void multiSourceLinkFilterTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/multiSource/").getFile();
    Graph<Long, ObjectMap, ObjectMap> inputGraph
        = new JSONDataSource(graphPath, true, env)
        .getGraph();

    assertEquals(3, inputGraph.getEdges().count());

    LinkFilter linkFilter = new LinkFilter
        .LinkFilterBuilder()
        .setEnvironment(env)
        .setRemoveIsolatedVertices(false)
        .setDataSources(Constants.GEO_SOURCES)
        .setStrategy(LinkFilterStrategy.BASIC)
        .build();

    Graph<Long, ObjectMap, ObjectMap> resultGraph = inputGraph
        .run(linkFilter);

    for (Tuple2<Long, Long> edge : resultGraph.getEdgeIds().collect()) {
      if (edge.f0 == 4L) {
        assertEquals(10L, edge.f1.longValue());
      } else {
        assertFalse(true);
      }
    }
  }

  /**
   * EqualDataSourceLinkRemover not tested, but tested separately
   */
  @Test
  public void defaultPreprocessingTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/defaultPreprocessing/").getFile();
    Graph<Long, ObjectMap, NullValue> inGraph = new JSONDataSource(graphPath, true, env)
        .getGraph(ObjectMap.class, NullValue.class);

    DataSet<Vertex<Long, ObjectMap>> vertices = inGraph
        .run(new DefaultPreprocessing(env))
        .getVertices();

    // both 59 and 84 are from LGD, this is correct, because the link filter
    // only affects direct links and not sth like 84 <-- 58 --> 59
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      assertTrue(vertex.getId() == 1375705L
      || vertex.getId() == 617158L
      || vertex.getId() == 617159L
      || vertex.getId() == 1022884L);
    }
  }

  /**
   * Link to vertex 1 is deleted
   */
  @Test
  public void typeMisMatchCorrectionTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/defaultPreprocessing/").getFile();
    Graph<Long, ObjectMap, NullValue> graph = new JSONDataSource(graphPath, true, env)
        .getGraph(ObjectMap.class, NullValue.class)
        .mapVertices(new InternalTypeMapFunction());

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
    env = TestBase.setupLocalEnvironment();
    String graphPath = PreprocessingTest.class
        .getResource("/data/representative/mergeExec").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph
        = new JSONDataSource(graphPath, true, env).getGraph();

    assertEquals(11, graph.getVertices().count());

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices()
        .runOperation(new IsolatedVertexRemover<>(graph.getEdges()));

    assertEquals(2, resultVertices.count());
  }
}
