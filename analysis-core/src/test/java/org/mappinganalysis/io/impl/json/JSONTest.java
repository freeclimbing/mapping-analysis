package org.mappinganalysis.io.impl.json;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.s1ck.gdl.GDLHandler;

import java.util.Collection;

public class JSONTest {
  private static final Logger LOG = Logger.getLogger(JSONTest.class);

  private ExecutionEnvironment env;

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
  public void readJSONTest() throws Exception {
    /**
     * Read file
     */
    env = ExecutionEnvironment.getExecutionEnvironment();
    String vertexInFile =
        JSONTest.class.getResource("/data/vertices.json").getFile();
    String edgeInFile =
        JSONTest.class.getResource("/data/edges.json").getFile();
    JSONDataSource dataSource = new JSONDataSource(vertexInFile, edgeInFile, env);

    Graph<Long, ObjectMap, ObjectMap> graph = dataSource.getGraph();

    // todo do sth

    /**
     * Write graph to JSON file
     */
    String tmpDir = temporaryFolder.getRoot().toString();
    String vertexOutFile = tmpDir + "/outVertices.json";
    String edgeOutFile = tmpDir + "/outEdges.json";
    JSONDataSink dataSink = new JSONDataSink(vertexOutFile, edgeOutFile);

    dataSink.writeGraph(graph);

    /**
     * todo better compare in and out file?
     */
    JSONDataSource testSource = new JSONDataSource(vertexOutFile, edgeOutFile, env);

    Graph<Long, ObjectMap, ObjectMap> inOutGraph = testSource.getGraph();

    DataSet<Vertex<Long, ObjectMap>> inVertices = graph.getVertices();
    for (Vertex<Long, ObjectMap> vertex : inVertices.collect()) {
      LOG.info("in result: " + vertex);
    }

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