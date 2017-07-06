package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Either return the weighted aggregated similarity or return mean similarity of all existing properties
 *
 * used in general merge and rejoin vertices from simsort
 */
public class AggSimValueTripletMapFunction
    implements MapFunction<Triplet<Long, ObjectMap, ObjectMap>, Triplet<Long, ObjectMap, ObjectMap>> {
  private final boolean ignoreMissingProperties;
  private final Double minSim;

  public AggSimValueTripletMapFunction(boolean ignoreMissingProperties, Double minSim) {
    this.ignoreMissingProperties = ignoreMissingProperties;
    this.minSim = minSim;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    ObjectMap value = triplet.getEdge().getValue();

    double aggregatedSim;
    if (ignoreMissingProperties) {
      if ((double) value.get(Constants.SIM_LABEL) < minSim) {
        aggregatedSim = 0D;
      } else {
        aggregatedSim = value.runOperation(new MeanAggregationFunction())
            .getEdgeSimilarity();
      }
    } else {
      aggregatedSim = SimilarityComputation.getWeightedAggSim(value);
    }

    value.setEdgeSimilarity(aggregatedSim);
    return triplet;
  }
}
