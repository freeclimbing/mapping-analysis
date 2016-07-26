package org.mappinganalysis.io.impl.json;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.mappinganalysis.model.ObjectMap;

public class JSONDataSink {

  private final String vertexPath;
  private final String edgePath;

  public JSONDataSink(String vertexPath, String edgePath) {
    this.vertexPath = vertexPath;
    this.edgePath = edgePath;
  }

  public <EV> void writeGraph(Graph<Long, ObjectMap, EV> graph) {
    graph.getVertices()
        .writeAsFormattedText(vertexPath,
            FileSystem.WriteMode.OVERWRITE,
            new VertexToJSONFormatter<>());
    graph.getEdges()
        .writeAsFormattedText(edgePath,
            FileSystem.WriteMode.OVERWRITE,
            new EdgeToJSONFormatter<>());
  }
}
