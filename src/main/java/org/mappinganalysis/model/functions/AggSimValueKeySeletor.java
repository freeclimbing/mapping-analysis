package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class AggSimValueKeySeletor implements KeySelector<Vertex<Long, ObjectMap>, Double> {
  @Override
  public Double getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
    return (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE);
  }
}
