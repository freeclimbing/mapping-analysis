package org.mappinganalysis.util.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class OldHashCcFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().containsKey(Constants.OLD_HASH_CC);
  }
}
