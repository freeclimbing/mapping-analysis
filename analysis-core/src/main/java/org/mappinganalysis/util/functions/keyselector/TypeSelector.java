package org.mappinganalysis.util.functions.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.util.Set;

public class TypeSelector implements KeySelector<Vertex<Long, ObjectMap>, Set<String>> {
  @Override
  public Set<String> getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().getTypes(Constants.TYPE_INTERN);
  }
}
