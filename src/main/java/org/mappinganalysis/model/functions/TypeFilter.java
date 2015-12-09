package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

/**
 * Filter resulting triplets based on type equality.
 */
public class TypeFilter implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
  @Override
  public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> weightedTriplet) throws Exception {
    Map<String, Object> props = weightedTriplet.getEdge().getValue();
    return props.containsKey(Utils.TYPE_MATCH) && (float) props.get(Utils.TYPE_MATCH) == 1f;
  }
}
