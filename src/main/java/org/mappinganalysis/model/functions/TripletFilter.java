package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.FlinkVertex;

import java.util.Map;

/**
 * Exact match triplet filter, soon deprecated!?
 */
public class TripletFilter implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
  @Override
  public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> weightedTriplet) throws Exception {
    Map<String, Object> props = weightedTriplet.getEdge().getValue();
    return props.containsKey("trigramSim") && (float) props.get("trigramSim") > 0.6f;
  }
}
