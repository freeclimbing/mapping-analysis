package org.mappinganalysis.model.functions.merge;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;

/**
 * Geo merge
 */
public class GeoMergeSimilarity
    extends SimilarityFunction<MergeTriplet, MergeTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(GeoMergeSimilarity.class);
  AggregationMode<MergeTriplet> mode;

  public GeoMergeSimilarity(AggregationMode<MergeTriplet> mode) {
    this.mode = mode;
  }

  @Override
  public MergeTriplet map(MergeTriplet triplet) throws Exception {
    Double labelSimilarity = getAttributeSimilarity(Constants.LABEL, triplet);

    HashMap<String, Double> values = Maps.newHashMap();
    values.put(Constants.LABEL, labelSimilarity);

    triplet.setSimilarity(mode.compute(values));

    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeTriplet triplet) {
//    triplet.getSrcTuple()

    double similarity = 0D;
//    Utils.getTrigramMetricAndSimplifyStrings()
//        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());
    BigDecimal tmpResult = new BigDecimal(similarity);

    return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
}
