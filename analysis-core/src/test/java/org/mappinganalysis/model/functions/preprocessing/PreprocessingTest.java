package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.DataLoader;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import static org.junit.Assert.assertEquals;

public class PreprocessingTest {
  private static final Logger LOG = Logger.getLogger(PreprocessingTest.class);
  private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void linkFilterStrategyTest() throws Exception {
    String graphPath = PreprocessingTest.class
        .getResource("/data/preprocessing/linkFilter/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = Utils.readFromJSONFile(graphPath, env, true);

    graph = Preprocessing.applyLinkFilterStrategy(graph, env, true, true);
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
