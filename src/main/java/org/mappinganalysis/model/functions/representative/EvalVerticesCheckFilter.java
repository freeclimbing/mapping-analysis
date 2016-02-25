package org.mappinganalysis.model.functions.representative;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

import java.util.ArrayList;

public class EvalVerticesCheckFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
  private final ArrayList<Long> vertexList;

  public EvalVerticesCheckFilter(ArrayList<Long> vertexList) {
    this.vertexList = vertexList;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    for (Long checkValue : vertexList) {
      if (vertex.getValue().getVerticesList().contains(checkValue)) {
        return true;
      }
    }
    return false;
  }
}
