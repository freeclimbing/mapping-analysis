package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;

public class JSONDataSource {
  private static final Logger LOG = Logger.getLogger(JSONDataSource.class);

  private final String vertexPath;
  private final String edgePath;
  private final ExecutionEnvironment environment;

  public JSONDataSource(String vertexPath, String edgePath, ExecutionEnvironment environment) {
    this.vertexPath = vertexPath;
    this.edgePath = edgePath;
    this.environment = environment;
  }

  public JSONDataSource(String vertexPath, ExecutionEnvironment environment) {
    this.vertexPath = vertexPath;
    this.environment = environment;
    this.edgePath = null;
  }

  public <VV, EV> Graph<Long, VV, EV> getGraph(Class<VV> vertexClass, Class<EV> edgeClass) {

    DataSet<Vertex<Long, VV>> vertices = environment.readTextFile(vertexPath)
        .map(new JSONToVertexFormatter<>(vertexClass));
    DataSet<Edge<Long, EV>> edges = null;
    if (edgePath != null) {
       edges = environment.readTextFile(edgePath)
          .map(new JSONToEdgeFormatter<>(edgeClass));
    }

    return Graph.fromDataSet(vertices, edges, environment);

  }

  public <VV> DataSet<Vertex<Long, VV>> getVertices(Class<VV> vertexClass) {
    return environment.readTextFile(vertexPath)
        .map(new JSONToVertexFormatter<>(vertexClass));
  }
}
