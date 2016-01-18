package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * similarity filter function - filter values below threshold
 */
public class TrigramSimilarityFilter implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> weightedTriplet) throws Exception {
    ObjectMap props = weightedTriplet.getEdge().getValue();
    return props.containsKey(Utils.TRIGRAM) && (float) props.get(Utils.TRIGRAM) > Utils.TRIGRAM_INITIAL_THRESHOLD;
  }
}
