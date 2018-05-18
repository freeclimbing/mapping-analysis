package org.mappinganalysis.model.functions.simcomputation;

import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.api.CustomOperation;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Compute the mean similarity for a set of properties. Use all {@value Constants#SIM_VALUES}
 * options given here.
 */
public class MeanAggregationFunction implements CustomOperation<ObjectMap>, Serializable {
  private static final Logger LOG = Logger.getLogger(MeanAggregationFunction.class);

  private ObjectMap properties;

  @Override
  public void setInput(ObjectMap properties) {
    this.properties = properties;
  }

  @Override
  public ObjectMap createResult() {
    double aggregatedSim = 0;
    int propCount = 0;

    for (String simKey : Constants.SIM_VALUES) {
      if (properties.containsKey(simKey)) {
        ++propCount;
        aggregatedSim += (double) properties.get(simKey);
        // remove unneeded single similarity value
        properties.remove(simKey);
      }
    }

    if (propCount == 0) { // will never happen
      properties.setEdgeSimilarity(0D);
    } else {
      BigDecimal result = new BigDecimal(aggregatedSim / propCount);
      result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

      properties.setEdgeSimilarity(result.doubleValue());
    }

    return properties;
  }
}
