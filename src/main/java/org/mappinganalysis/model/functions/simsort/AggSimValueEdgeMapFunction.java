package org.mappinganalysis.model.functions.simsort;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Aggregate all similarity values, either based on weight based metric
 * or simply by existence (missing properties are ignored).
 */
public class AggSimValueEdgeMapFunction implements MapFunction<Edge<Long, ObjectMap>, Edge<Long, ObjectMap>> {

  private static final Logger LOG = Logger.getLogger(AggSimValueEdgeMapFunction.class);
  private final boolean ignoreMissingProperties;

  public AggSimValueEdgeMapFunction(boolean ignoreMissingProperties) {
    this.ignoreMissingProperties = ignoreMissingProperties;
  }

  @Override
  public Edge<Long, ObjectMap> map(Edge<Long, ObjectMap> edge) throws Exception {
    ObjectMap value = edge.getValue();

    double aggregatedSim;
    if (ignoreMissingProperties) {
      aggregatedSim = getAggSim(value);
    } else {
      aggregatedSim = getWeightedAggSim(value);
    }

    value.put(Utils.AGGREGATED_SIM_VALUE, aggregatedSim);
    return edge;
  }

  /**
   * Compose similarity values based on existence: if property is missing, its not considered at all.
   * @param value property map
   * @return aggregated similarity value
   */
  private double getAggSim(ObjectMap value) {
    double aggregatedSim = 0;
    int propCount = 0;
    if (value.containsKey(Utils.TRIGRAM)) {
      ++propCount;
      aggregatedSim = (float) value.get(Utils.TRIGRAM);
    }
    if (value.containsKey(Utils.TYPE_MATCH)) {
      ++propCount;
      aggregatedSim += (float) value.get(Utils.TYPE_MATCH);
    }
    if (value.containsKey(Utils.DISTANCE)) {
      double distanceSim = getDistanceValue(value);
      if (Doubles.compare(distanceSim, -1) > 0) {
        aggregatedSim += distanceSim;
        ++propCount;
      }
    }

    return aggregatedSim / propCount;
  }

  /**
   * Compose similarity values based on weights for each of the properties, missing values are counted as zero.
   * @param value property map
   * @return aggregated similarity value
   */
  private double getWeightedAggSim(ObjectMap value) {
    double trigramWeight = 0.45;
    double typeWeight = 0.25;
    double geoWeight = 0.3;
    double aggregatedSim;
    if (value.containsKey(Utils.TRIGRAM)) {
      aggregatedSim = trigramWeight * (float) value.get(Utils.TRIGRAM);
    } else {
      aggregatedSim = 0;
    }
    if (value.containsKey(Utils.TYPE_MATCH)) {
      aggregatedSim += typeWeight * (float) value.get(Utils.TYPE_MATCH);
    }
    if (value.containsKey(Utils.DISTANCE)) {
      double distanceSim = getDistanceValue(value);
      if (Doubles.compare(distanceSim, -1) > 0) {
        aggregatedSim += geoWeight * distanceSim;
      }
    }
    return aggregatedSim;
  }

  /**
   * get distance property from object map
   * @param value object map
   * @return distance
   */
  private double getDistanceValue(ObjectMap value) {
    Object object = value.get(Utils.DISTANCE);
    if (object instanceof Double) {
      return (Double) object;
    } else {
      LOG.info("Error (should not occur)" + object.getClass().toString());
      return -1;
    }
    //      aggregatedSim += geoWeight * (Double) value.get(Utils.DISTANCE); // TODO why is this not working?
  }
}
