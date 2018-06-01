package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.math.BigDecimal;

public class TripletMeanAggregationFunction implements MapFunction<Triplet<Long,ObjectMap,ObjectMap>, Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    double aggregatedSim = 0;
    int propCount = 0;

    for (String simKey : Constants.SIM_VALUES) {
      if (triplet.getEdge().getValue().containsKey(simKey)) {
        ++propCount;
        aggregatedSim += (double) triplet.getEdge().getValue().get(simKey);
        // remove unneeded single similarity value
        triplet.getEdge().getValue().remove(simKey);
      }
    }

    BigDecimal result = new BigDecimal(aggregatedSim / propCount);
    result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

    triplet.getEdge().getValue().setEdgeSimilarity(result.doubleValue());

    return triplet;
  }
}
