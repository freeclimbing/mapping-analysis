package org.mappinganalysis.model.functions.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ActivateAllVerticesMapFunction implements MapFunction<Vertex<Long, ObjectMap>, ObjectMap> {
  @Override
  public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
    vertex.getValue().put(Utils.VERTEX_STATUS, Boolean.TRUE);
    return vertex.getValue();
  }
}
