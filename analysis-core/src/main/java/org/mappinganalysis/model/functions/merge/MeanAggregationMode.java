package org.mappinganalysis.model.functions.merge;

import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Mean aggregation used in Merge.
 * if only label sim is available, it is set to 0 if sim is below 0.7
 */
public class MeanAggregationMode<T>
    extends AggregationMode<MergeTriplet<T>>
    implements Serializable{

  @Override
  public Double compute(HashMap<String, Double> values) {
    Double geoSimilarity = values.get(Constants.GEO);
    Double labelSimilarity = values.get(Constants.LABEL);

    if (geoSimilarity != null) {
      return (geoSimilarity + labelSimilarity) / 2;
    } else {
      // TODO fix this
      if (labelSimilarity < 0.7) {
        return 0D;
      } else {
        return labelSimilarity;
      }
    }
  }
}
