package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.model.ObjectMap;

import static org.junit.Assert.assertTrue;

public class JSONTest {
  private static final Logger LOG = Logger.getLogger(JSONTest.class);

  private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  // TODO no duplicate keys for properties in gdl (only with graph properties)
  private static final String EXAMPLE = "g[" +
      "(v1 {label = \"Kathmandu\", typeIntern = \"Settlement\", ccId = 7380L, " +
      "ontology = \"http://sws.geonames.org/\", lon = 85.3206D, lat = 27.7017D})" +
      "(v2 {label = \"Katmandu (Nepal)\", typeIntern = \"no_type\", ccId = 108L})" +
      "(v2)-[e1:sameAs {aggSimValue = 0.9428090453147888D}]->(v1)" +
      "]";

  @Test
  public void pathTest() {
    JSONDataSource jsonDataSource = new JSONDataSource("path", "step", false, env);
    assertTrue(jsonDataSource.getVertexPath().equals("path/output/step/vertices/"));
    assertTrue(jsonDataSource.getEdgePath().equals("path/output/step/edges/"));

    jsonDataSource = new JSONDataSource("path", "", false, env);
    assertTrue(jsonDataSource.getVertexPath().equals("path/input/vertices/"));
    jsonDataSource = new JSONDataSource("path", null, false, env);
    assertTrue(jsonDataSource.getVertexPath().equals("path/input/vertices/"));

    jsonDataSource = new JSONDataSource("path", "", true, env);
    assertTrue(jsonDataSource.getVertexPath().equals("path/vertices/"));
  }

  /**
   * Simple JSON input reader test
   * @throws Exception
   */
  @Test
  public void readJSONTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String path = JSONTest.class.getResource("/data/preprocessing/input/").getFile();

    Graph<Long, ObjectMap, NullValue> graph = new JSONDataSource(path, true, env)
        .getGraph(ObjectMap.class, NullValue.class);
    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices();
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      assertTrue(vertex.getId() == 109L
          || vertex.getId() == 7380L
          || vertex.getId() == 5346L);
      assertTrue(vertex.getValue().getClass().equals(ObjectMap.class));
    }

    for (Edge<Long, NullValue> edge : graph.getEdges().collect()) {
      assertTrue(edge.getSource() == 7380L);
      assertTrue(edge.getTarget() == 5346L
          || edge.getTarget() == 109L);
      assertTrue(edge.getValue().getClass().equals(NullValue.class));
    }
  }

  /**
   * not working
   * @throws Exception
   */
  @Test
  public void readWriteJSONTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String path = JSONTest.class.getResource("/data/preprocessing/input/").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph = new JSONDataSource(path, true, env).getGraph();

    /**
     * Write graph to JSON file
     */
    String tmpDir = temporaryFolder.getRoot().toString();
    String step = "testStep";
    JSONDataSink dataSink = new JSONDataSink(tmpDir, step);

    dataSink.writeGraph(graph);
    env.execute();

    /**
     * Read again from tmp folder
     */
    Graph<Long, ObjectMap, ObjectMap> inOutGraph = new JSONDataSource(tmpDir, step, env)
        .getGraph();

    DataSet<Vertex<Long, ObjectMap>> inVertices = graph.getVertices();
    for (Vertex<Long, ObjectMap> vertex : inVertices.collect()) {
      LOG.info("inn result: " + vertex);
    }

//    // ?? TODO
//    GraphEquality equality = new GraphEquality(
//        new GraphHeadToDataString(),
//        new VertexToDataString(),
//        new EdgeToDataString(),
//        false
//    );
//
//
//    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
//    LogicalGraph.fromDataSets(graph.getVertices(), graph.getEdges(), config);

    /**
     * todo better compare in and out file?
     *
     * todo actual test missing
     */
    DataSet<Vertex<Long, ObjectMap>> outVertices = inOutGraph.getVertices();
    for (Vertex<Long, ObjectMap> vertex : outVertices.collect()) {
      LOG.info("out result: " + vertex);
    }
//    DataSet<Edge<Long, ObjectMap>> mergedClusterEdges = graph.getEdges();
//    for (Edge<Long, ObjectMap> edge : mergedClusterEdges.collect()) {
//      LOG.info("resultEdge: " + edge);
//    }
  }
}