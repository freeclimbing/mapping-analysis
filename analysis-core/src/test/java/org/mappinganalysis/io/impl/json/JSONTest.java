package org.mappinganalysis.io.impl.json;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSATriangleCount;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.MappingAnalysisExampleTest;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.s1ck.gdl.GDLHandler;

import java.util.Collection;

public class JSONTest {
  private static final Logger LOG = Logger.getLogger(JSONTest.class);

  private ExecutionEnvironment env;


  // TODO no duplicate keys for properties in gdl (only with graph properties)
  private static final String EXAMPLE = "g[" +
      "(v1 {label = \"Kathmandu\", typeIntern = \"Settlement\", ccId = 7380L, " +
      "ontology = \"http://sws.geonames.org/\", lon = 85.3206D, lat = 27.7017D})" +
      "(v2 {label = \"Katmandu (Nepal)\", typeIntern = \"no_type\", ccId = 108L})" +
      "(v2)-[e1:sameAs {aggSimValue = 0.9428090453147888D}]->(v1)" +
      "]";

  @Test
  public void readJSONTest() throws Exception {
    env = ExecutionEnvironment.getExecutionEnvironment();
    String vertexFile =
        JSONTest.class.getResource("/data/vertices.json").getFile();
    String edgeFile =
        JSONTest.class.getResource("/data/edges.json").getFile();

    JSONDataSource dataSource = new JSONDataSource(vertexFile, edgeFile, env);
    Graph<Long, ObjectMap, ObjectMap> graph = dataSource.getGraph();

    DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices = graph.getVertices();

//    Collection<Vertex<Long, ObjectMap>> loadedVertices = Lists.newArrayList();
//    graph.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
//    env.execute();

    for (Vertex<Long, ObjectMap> vertex : mergedClusterVertices.collect()) {
      LOG.info("result: " + vertex);
    }
    DataSet<Edge<Long, ObjectMap>> mergedClusterEdges = graph.getEdges();
    for (Edge<Long, ObjectMap> edge : mergedClusterEdges.collect()) {
      LOG.info("resultEdge: " + edge);
    }
  }

  @Test
  public void writeJSONTest() throws Exception {
    env = ExecutionEnvironment.getExecutionEnvironment();
    Constants.VERBOSITY = Constants.DEBUG;

    GDLHandler handler = new GDLHandler.Builder().buildFromString(EXAMPLE);
    Graph<Long, ObjectMap, ObjectMap> graph = MappingAnalysisExampleTest.createTestGraph(handler);

    DataSet<Vertex<Long, ObjectMap>> mergedClusterVertices = graph.getVertices();

    Utils.writeToJSONFile(graph, "output/testtmp");

    Collection<Vertex<Long, ObjectMap>> loadedVertices = Lists.newArrayList();
    graph.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    env.execute();

//    for (Vertex<Long, ObjectMap> vertex : mergedClusterVertices.collect()) {
//      LOG.info("result: " + vertex);
//    }
  }
}