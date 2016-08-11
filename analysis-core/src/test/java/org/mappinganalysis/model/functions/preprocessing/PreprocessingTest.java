package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.DataLoader;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PreprocessingTest {
  private static final Logger LOG = Logger.getLogger(PreprocessingTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void compSourceTupleTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    DataSet<ComponentSourceTuple> resultTuples = Preprocessing
        .getComponentSourceTuples(graph.getVertices(), null);

    for (ComponentSourceTuple result : resultTuples.collect()) {
      assertEquals(60190L, result.getCcId().longValue());
      assertEquals(5, result.getSourceCount().intValue());
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
  public void linkFilterStrategyTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    graph = Preprocessing.applyLinkFilterStrategy(graph, env, true);
//    graph.getEdges().print(); // 2 edges
//    graph.getVertices().print(); // 4 vertices

    graph = graph.filterOnEdges(new FilterFunction<Edge<Long, ObjectMap>>() {
      @Override
      public boolean filter(Edge<Long, ObjectMap> edge) throws Exception {

        if (edge.getSource() == 617158L) {
          assertEquals(617159L, edge.getTarget().longValue());
          return true;
        } else if (edge.getSource() == 1022884L) {
          assertEquals(1375705L, edge.getTarget().longValue());
          return true;
        } else {
          return false;
        }
      }
    });

    assertEquals(4, graph.getVertices().count());
  }

  @Test
  public void nullValueTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/nullValueLinkFilter/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    graph = Preprocessing.applyLinkFilterStrategy(graph, env, true);
//    graph.getEdges().print(); // 2 edges
//    graph.getVertices().print(); // 4 vertices

    graph.getVertices().print();
  }


  @Test
  public void finalOneToManyTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);
    graph = SimilarityComputation.computeTransitiveClosureEdgeSimilarities(graph, env);
    assertEquals(21, graph.getEdgeIds().count());

    graph = SimilarityComputation.removeOneToManyVertices(graph, env);
    for (Vertex<Long, ObjectMap> vertex : graph.getVertices().collect()) {
      LOG.info(vertex.toString());
      assertTrue(vertex.getId() == 60191L
          || vertex.getId() == 252016L
          || vertex.getId() == 513732L
          || vertex.getId() == 60190L
          || vertex.getId() == 1268005L);
    }
  }

  @Test
  public void oneToManyTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/oneToMany/").getFile();
    Graph<Long, ObjectMap, ObjectMap> inputGraph = Utils.readFromJSONFile(graphPath, env, true);

    Graph<Long, ObjectMap, ObjectMap> resultDeleteVerticesGraph = Preprocessing
        .applyLinkFilterStrategy(inputGraph, env, true);

    for (Tuple2<Long, Long> edge : resultDeleteVerticesGraph.getEdgeIds().collect()) {
      assertEquals(2642L, edge.f0.longValue());
      assertEquals(46584L, edge.f1.longValue());
    }

    for (Long vertex : resultDeleteVerticesGraph.getVertexIds().collect()) {
      assertTrue(vertex == 2642L || vertex == 46584L);
    }

    Graph<Long, ObjectMap, ObjectMap> resultNoDeleteVerticesGraph = Preprocessing
        .applyLinkFilterStrategy(inputGraph, env, false);

    for (Tuple2<Long, Long> edge : resultNoDeleteVerticesGraph.getEdgeIds().collect()) {
      assertEquals(2642L, edge.f0.longValue());
      assertEquals(46584L, edge.f1.longValue());
    }

    assertEquals(7, resultNoDeleteVerticesGraph.getVertices().count());
  }

  /**
   * TODO fix this test
   * @throws Exception
   */
  @Test
  public void generalPreprocessingTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/general/").getFile();
    Graph<Long, ObjectMap, ObjectMap> inGraph = Utils.readFromJSONFile(graphPath, env, true);

    DataSet<Edge<Long, NullValue>> edges = inGraph.getEdges()
        .map(edge -> {
          LOG.info("edge: " + edge.getSource() + " " + edge.getTarget());
          return new Edge<>(edge.getSource(), edge.getTarget(), NullValue.getInstance());
        })
        .returns(new TypeHint<Edge<Long, NullValue>>() {});

    Graph<Long, ObjectMap, NullValue> graph = Graph.fromDataSet(inGraph.getVertices(), edges, env);

//    graph = Preprocessing.applyTypeMissMatchCorrection(graph, true, env);

    Graph<Long, ObjectMap, ObjectMap> simGraph = Graph.fromDataSet(
        graph.getVertices(),
        SimilarityComputation.computeGraphEdgeSim(graph, Constants.DEFAULT_VALUE),
        env);

    simGraph = Preprocessing.applyLinkFilterStrategy(simGraph, env, true);

    simGraph.getVertices().print();

//    DataSet<Vertex<Long, ObjectMap>> result = Preprocessing.deleteVerticesWithoutAnyEdges(
//        inGraph.getVertices(),
//        inGraph.getEdges().<Tuple2<Long, Long>>project(0, 1));
//
//    assertEquals(4, result.count());
  }

  @Test
  public void deleteVerticesWithoutEdgesTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/deleteVerticesWithoutEdges/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    DataSet<Vertex<Long, ObjectMap>> result = Preprocessing.deleteVerticesWithoutAnyEdges(
        graph.getVertices(),
        graph.getEdges().<Tuple2<Long, Long>>project(0, 1));

    assertEquals(4, result.count());
  }

  @Test
  //AskTimeoutException
  public void getInputGraphFromCSVTest() throws Exception {
    DataLoader loader = new DataLoader(env);
    final String vertexFile = "concept.csv";
    final String propertyFile = "concept_attributes.csv";
    final String path = PreprocessingTest.class
        .getResource("/data/preprocessing/tmp/").getFile();

    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVerticesFromCsv(path.concat(vertexFile), path.concat(propertyFile));


    DataSet<Vertex<Long, ObjectMap>> nytFbVertices = vertices.filter(vertex ->
        vertex.getValue().getOntology().equals(Constants.NYT_NS));

    LOG.info(nytFbVertices.count());
  }
}
