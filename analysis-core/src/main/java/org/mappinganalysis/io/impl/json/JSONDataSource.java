package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

public class JSONDataSource {

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

  public Graph<Long, ObjectMap, ObjectMap> getGraph() {
    DataSet<Vertex<Long, ObjectMap>> vertices = environment.readTextFile(vertexPath)
        .map(new JSONToVertexFormatter());
    DataSet<Edge<Long, ObjectMap>> edges = environment.readTextFile(edgePath)
        .map(new JSONToEdgeFormatter());

    return Graph.fromDataSet(vertices, edges, environment);
  }

  public DataSet<Vertex<Long, ObjectMap>> getVertices() {
    return environment.readTextFile(vertexPath)
        .map(new JSONToVertexFormatter());
  }
}
