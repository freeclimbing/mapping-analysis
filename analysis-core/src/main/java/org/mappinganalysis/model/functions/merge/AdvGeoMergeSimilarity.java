package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Maps;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;

/**
 * Advanced Geo Merge Similarity Computation, not yet working.
 */
@Deprecated
public class AdvGeoMergeSimilarity
    extends SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet>
    implements Serializable {
  AggregationMode<MergeGeoTriplet> mode;

  public AdvGeoMergeSimilarity(AggregationMode<MergeGeoTriplet> mode) {
    this.mode = mode;
  }

  @Override
  public MergeGeoTriplet map(MergeGeoTriplet triplet) throws Exception {
    Double labelSimilarity = getAttributeSimilarity(Constants.LABEL, triplet);

    HashMap<String, Double> values = Maps.newHashMap();
    values.put(Constants.LABEL, labelSimilarity);

    triplet.setSimilarity(mode.compute(values));

    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeGeoTriplet triplet) {
//    triplet.getSrcTuple()

    double similarity = 0D;
//    Utils.getCosineTrigramMetric()
//        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());
    BigDecimal tmpResult = new BigDecimal(similarity);

    return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
}
