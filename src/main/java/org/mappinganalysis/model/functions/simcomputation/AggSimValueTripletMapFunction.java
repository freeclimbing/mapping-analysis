package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Either return the weighted aggregated similarity or return mean similarity of all existing properties
 */
public class AggSimValueTripletMapFunction implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Triplet<Long, ObjectMap, ObjectMap>> {
  private final boolean ignoreMissingProperties;

  public AggSimValueTripletMapFunction(boolean ignoreMissingProperties) {
    this.ignoreMissingProperties = ignoreMissingProperties;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    ObjectMap value = triplet.getEdge().getValue();

    double aggregatedSim;
    if (ignoreMissingProperties) {
      aggregatedSim = SimCompUtility.getMeanSimilarity(value);
    } else {
      aggregatedSim = SimCompUtility.getWeightedAggSim(value);
    }

    value.put(Utils.AGGREGATED_SIM_VALUE, aggregatedSim);
    return triplet;
  }
}
