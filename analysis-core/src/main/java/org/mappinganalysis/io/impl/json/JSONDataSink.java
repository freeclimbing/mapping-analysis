package org.mappinganalysis.io.impl.json;

import org.apache.flink.graph.Graph;
import org.mappinganalysis.model.ObjectMap;

public class JSONDataSink {

  private final String vertexPath;
  private final String edgePath;

  public JSONDataSink(String vertexPath, String edgePath) {
    this.vertexPath = vertexPath;
    this.edgePath = edgePath;
  }

  public void writeGraph(Graph<Long, ObjectMap, ObjectMap> graph) {
    graph.getVertices()
        .writeAsFormattedText(vertexPath, new VertexToJSONFormatter<>());
    graph.getEdges()
        .writeAsFormattedText(edgePath, new EdgeToJSONFormatter<>());
  }
}
