package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class JSONDataSource {
  private static final Logger LOG = Logger.getLogger(JSONDataSource.class);

  private final String vertexPath;
  private final String edgePath;
  private final ExecutionEnvironment environment;

  /**
   * Default constructor for reading input graph, files in directory "input"
   * all vertices.json files are in subdirectory vertices
   * all edges.json files are in subdirectory edges
   */
  public JSONDataSource(String path, ExecutionEnvironment environment) {
    this(path, false, environment);
  }

  /**
   * Constructor for data source, optionally with absolute file path (tests),
   * all vertices.json files are in subdirectory vertices
   * all edges.json files are in subdirectory edges
   */
  public JSONDataSource(String path, Boolean isAbsolutePath, ExecutionEnvironment environment) {
    this(path, null, isAbsolutePath, environment);
  }

  /**
   * Constructor with all parameters
   * @param path path to files
   * @param step name of current working step
   * @param isAbsolutePath absolute is for testing only
   * @param environment env
   */
  public JSONDataSource(String path, String step, Boolean isAbsolutePath, ExecutionEnvironment environment) {
    this.environment = environment;
    if (!path.endsWith(Constants.SLASH)) {
      path = path.concat(Constants.SLASH);
    }
    if (isAbsolutePath) { // no need to care for step
      this.vertexPath = path.concat(Constants.VERTICES);
      this.edgePath = path.concat(Constants.EDGES);
    } else if (step == null || step.isEmpty()) { // only initial input step reads from input directory
      vertexPath = path.concat(Constants.INPUT.concat(Constants.VERTICES));
      edgePath = path.concat(Constants.INPUT.concat(Constants.EDGES));
    } else { // working steps read from output directory
      vertexPath = path.concat(Constants.OUTPUT)
          .concat(step).concat(Constants.SLASH)
          .concat(Constants.VERTICES);
      edgePath = path.concat(Constants.OUTPUT)
          .concat(step).concat(Constants.SLASH)
          .concat(Constants.EDGES);
    }
  }

  public String getVertexPath() {
    return vertexPath;
  }

  public String getEdgePath() {
    return edgePath;
  }

  /**
   * JSONDataSource for reading graphs from a previous step
   * @param path general path
   * @param step working step
   * @param env env
   */
  public JSONDataSource(String path, String step, ExecutionEnvironment env) {
    this(path, step, false, env);
  }

  /**
   * Generic return graph, specify return value
   * @param vertexClass vertex value to return
   * @param edgeClass edge value to return
   * @return generic graph
   */
  public <VV, EV> Graph<Long, VV, EV> getGraph(Class<VV> vertexClass, Class<EV> edgeClass) {

    DataSet<Vertex<Long, VV>> vertices = environment.readTextFile(vertexPath)
        .map(new JSONToVertexFormatter<>(vertexClass));
    DataSet<Edge<Long, EV>> edges = environment.readTextFile(edgePath)
          .map(new JSONToEdgeFormatter<>(edgeClass));

    return Graph.fromDataSet(vertices, edges, environment);
  }

  /**
   * Get default graph
   * @return both edge as well as vertex value are ObjectMap
   */
  public Graph<Long, ObjectMap, ObjectMap> getGraph() {
    return this.getGraph(ObjectMap.class, ObjectMap.class);
  }

  public <VV> DataSet<Vertex<Long, VV>> getVertices(Class<VV> vertexClass) {
    return environment.readTextFile(vertexPath)
        .map(new JSONToVertexFormatter<>(vertexClass));
  }

  /**
   * Default implementation, get ObjectMap value vertices
   */
  public DataSet<Vertex<Long, ObjectMap>> getVertices() {
    return this.getVertices(ObjectMap.class);
  }
}
