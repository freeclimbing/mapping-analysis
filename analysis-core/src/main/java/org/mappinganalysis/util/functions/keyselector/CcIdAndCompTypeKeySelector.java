package org.mappinganalysis.util.functions.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Key Selector to filter by component id and resource type.
 */
@Deprecated
public class CcIdAndCompTypeKeySelector implements KeySelector<Vertex<Long, ObjectMap>, Tuple2<Long, String>> {
  private static final Logger LOG = Logger.getLogger(CcIdAndCompTypeKeySelector.class);

  @Override
  public Tuple2<Long, String> getKey(Vertex<Long, ObjectMap> vertex) throws Exception {
//    LOG.info("###ccidcomp### SELECTOR: " + vertex.toString());
    return new Tuple2<>((long) vertex.getValue().get(Constants.CC_ID),
        vertex.getValue().get(Constants.COMP_TYPE).toString());
  }
}
