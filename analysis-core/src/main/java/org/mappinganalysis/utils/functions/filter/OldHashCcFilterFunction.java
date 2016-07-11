package org.mappinganalysis.utils.functions.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class OldHashCcFilterFunction implements FilterFunction<Vertex<Long, ObjectMap>> {
  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    return vertex.getValue().containsKey(Utils.OLD_HASH_CC);
  }
}
