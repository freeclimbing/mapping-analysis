package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Triplet;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

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
    boolean hasSimDist = edgeValue.containsKey(Constants.SIM_DISTANCE);
    boolean hasSimLabel = edgeValue.containsKey(Constants.SIM_TRIGRAM);
    boolean hasSimType = edgeValue.containsKey(Constants.SIM_TYPE);

    boolean hasHighThreshold = edgeValue.containsKey(Constants.AGGREGATED_SIM_VALUE)
        && edgeValue.getEdgeSimilarity() > threshold;
    boolean hasMinTwoSimValues = (hasSimDist && (hasSimLabel || hasSimType))
        || (hasSimLabel && hasSimType);

    boolean hasNoOrHighDistanceSim = !hasSimDist || ((double) edgeValue.get(Constants.SIM_DISTANCE) > 0.7);
    boolean hasNoOrHighTypeSim = !hasSimType || ((double) edgeValue.get(Constants.SIM_TYPE) > 0.7);
    boolean hasOnlyHighLabel = hasSimLabel && ((double) edgeValue.get(Constants.SIM_TRIGRAM) > 0.8);

    if (hasHighThreshold && hasMinTwoSimValues) {
      LOG.info("Merge " + triplet.getSrcVertex().getId() + " <-> " + triplet.getTrgVertex().getId() + " #### : \n"
          + triplet.getSrcVertex().getValue().toString() + "\n"
          + triplet.getTrgVertex().getValue().toString());
      return true;
    } else if (hasOnlyHighLabel && hasNoOrHighDistanceSim && hasNoOrHighTypeSim) {
      LOG.info("Merge HIGH LABEL" + triplet.getSrcVertex().getId() + " <-> " + triplet.getTrgVertex().getId() + " #### : \n"
          + triplet.getSrcVertex().getValue().toString() + "\n"
          + triplet.getTrgVertex().getValue().toString());
      return true;
    } else {
      return false;
    }

//    return hasHighThreshold && hasMinTwoSimValues;
  }
}
