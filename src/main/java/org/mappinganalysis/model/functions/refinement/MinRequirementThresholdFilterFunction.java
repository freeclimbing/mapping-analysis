package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Only filter triplets which have at least 2 similar enough
 * properties and a high enough aggregated similarity value.
 */
public class MinRequirementThresholdFilterFunction implements FilterFunction<Triplet<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MinRequirementThresholdFilterFunction.class);
  private final double threshold;

  public MinRequirementThresholdFilterFunction(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public boolean filter(Triplet<Long, ObjectMap, ObjectMap> triplet) throws Exception {
    ObjectMap edgeValue = triplet.getEdge().getValue();
    boolean hasSimDist = edgeValue.containsKey(Utils.SIM_DISTANCE);
    boolean hasSimLabel = edgeValue.containsKey(Utils.SIM_TRIGRAM);
    boolean hasSimType = edgeValue.containsKey(Utils.SIM_TYPE);

    boolean hasHighThreshold = edgeValue.containsKey(Utils.AGGREGATED_SIM_VALUE)
        && (double) edgeValue.get(Utils.AGGREGATED_SIM_VALUE) > threshold;
    boolean hasMinTwoSimValues = (hasSimDist && (hasSimLabel || hasSimType))
        || (hasSimLabel && hasSimType);

    if (hasHighThreshold && hasMinTwoSimValues) {
      LOG.info("Merge " + triplet.getSrcVertex().getId() + " <-> " + triplet.getTrgVertex().getId() + " #### : \n"
          + triplet.getSrcVertex().getValue().toString() + "\n"
          + triplet.getTrgVertex().getValue().toString());
      return true;
    } else {
      return false;
    }

//    return hasHighThreshold && hasMinTwoSimValues;
  }
}
