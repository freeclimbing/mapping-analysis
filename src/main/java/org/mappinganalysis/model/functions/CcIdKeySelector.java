package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * select strategy cc_id (e.g., for group by)
 */
public class CcIdKeySelector implements KeySelector<Vertex<Long, ObjectMap>, Long> {
  @Override
  public Long getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
    return (long) vertex.getValue().get(Utils.CC_ID);
  }
}
