package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.TypeDictionary;
import org.mappinganalysis.utils.Utils;

import java.util.Set;

public class AddShadingTypeMapFunction implements MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
    Set<String> types = vertex.getValue().getTypes(Utils.TYPE_INTERN);
    vertex.getValue().put(Utils.COMP_TYPE, Utils.getShadingTypes(types));

    return vertex;
  }
}
