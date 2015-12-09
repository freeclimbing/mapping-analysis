package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.FlinkVertex;
import org.mappinganalysis.utils.Utils;

import java.util.Map;

/**
 * Exact match triplet filter, soon deprecated!?
 */
public class TrigramSimilarityFilter implements FilterFunction<Triplet<Long, FlinkVertex, Map<String, Object>>> {
  @Override
  public boolean filter(Triplet<Long, FlinkVertex, Map<String, Object>> weightedTriplet) throws Exception {
    Map<String, Object> props = weightedTriplet.getEdge().getValue();
    return props.containsKey(Utils.TRIGRAM) && (float) props.get(Utils.TRIGRAM) > Utils.TRIGRAM_INITIAL_THRESHOLD;
  }
}
