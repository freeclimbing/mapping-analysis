package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Write Gelly graphs or vertices to JSON files
 */
public class JSONDataSink {

  private final String vertexPath;
  private final String edgePath;

  /**
   * Constructor for testing, fix appropriate test in PreprocessingTest
   */
  public JSONDataSink(String path) {
    this(path, null, true);
  }

  /**
   * Default constructor
   * @param path default file path
   * @param step step name
   */
  public JSONDataSink(String path, String step) {
    this(path, step, false);
  }

  private JSONDataSink(String path, String step, Boolean isAbsolutePath) {
    if (!path.endsWith(Constants.SLASH)) {
      path = path.concat(Constants.SLASH);
    }
    if (isAbsolutePath) { // no need to care for step
      this.vertexPath = path.concat(Constants.VERTICES);
      this.edgePath = path.concat(Constants.EDGES);
    } else {
      this.vertexPath = path.concat(Constants.OUTPUT)
          .concat(step).concat(Constants.SLASH)
          .concat(Constants.VERTICES);
      this.edgePath = path.concat(Constants.OUTPUT)
          .concat(step).concat(Constants.SLASH)
          .concat(Constants.EDGES);
    }
  }

  /**
   * TODO What happens with empty edge collection?
   */
  public <VV, EV> void writeGraph(Graph<Long, VV, EV> graph) {
    graph.getVertices()
        .writeAsFormattedText(vertexPath,
            FileSystem.WriteMode.OVERWRITE,
            new VertexToJSONFormatter<>());
    graph.getEdges()
        .writeAsFormattedText(edgePath,
            FileSystem.WriteMode.OVERWRITE,
            new EdgeToJSONFormatter<>());
  }

  public void writeVertices(DataSet<Vertex<Long, ObjectMap>> vertices) {
    vertices.writeAsFormattedText(vertexPath,
        FileSystem.WriteMode.OVERWRITE,
        new VertexToJSONFormatter<>());
  }

  public <T extends Tuple> void writeTuples(DataSet<T> tuples) {
    tuples.writeAsFormattedText(vertexPath,
        FileSystem.WriteMode.OVERWRITE,
        new TupleToJSONFormatter<>());
  }
}
