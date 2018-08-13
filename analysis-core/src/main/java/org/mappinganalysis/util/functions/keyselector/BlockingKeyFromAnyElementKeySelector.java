package org.mappinganalysis.util.functions.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;

public class BlockingKeyFromAnyElementKeySelector
    implements KeySelector<Triplet<Long,ObjectMap,ObjectMap>, String> {
  @Override
  public String getKey(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    return triplet.getSrcVertex().getValue().getBlockingKey();
  }
}
