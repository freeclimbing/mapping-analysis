package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.TypeDictionary;
import org.mappinganalysis.utils.Utils;

public class AddShadingTypeMapFunction implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    String vertexType = vertex.getValue().get(Utils.TYPE_INTERN).toString();
    vertex.getValue().put(Utils.COMP_TYPE, Utils.getShadingType(vertexType));

    return vertex;
  }
}
