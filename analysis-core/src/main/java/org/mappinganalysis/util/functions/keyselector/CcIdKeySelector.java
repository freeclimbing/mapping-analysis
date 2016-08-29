package org.mappinganalysis.util.functions.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Vertex;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Key Selector connected component id
 */
public class CcIdKeySelector implements KeySelector<Vertex<Long, ObjectMap>, Long> {
  @Override
  public Long getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
//    int tmp = (int) ; // TODO fix, needed for test
//    if (vertex.getValue().get(Constants.CC_ID) instanceof Integer) {
//      int tmp = (int) vertex.getValue().get(Constants.CC_ID);
//      return (long) tmp;
//    }
    Preconditions.checkNotNull(vertex.getValue().getCcId(), "ccid null for " + vertex.toString());
    return vertex.getValue().getCcId();
  }
}
