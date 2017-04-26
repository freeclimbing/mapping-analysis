package org.mappinganalysis.model.functions.merge;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;

/**
 * Add similarities to Merge Triplets based on property values.
 */
class MergeTripletGeoLabelSimilarity
    extends SimilarityFunction<MergeTriplet, MergeTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeTripletGeoLabelSimilarity.class);
  AggregationMode<MergeTriplet> mode;

  public MergeTripletGeoLabelSimilarity(AggregationMode<MergeTriplet> mode) {
    this.mode = mode;
  }

  // TODO add min sim check
  @Override
  public MergeTriplet map(MergeTriplet triplet) throws Exception {
    Double labelSimilarity = getLabelSimilarity(triplet.getSrcTuple().getLabel(),
        triplet.getTrgTuple().getLabel());

    Double geoSimilarity = getGeoSimilarity(triplet.getSrcTuple().getLatitude(),
        triplet.getSrcTuple().getLongitude(),
        triplet.getTrgTuple().getLatitude(),
        triplet.getTrgTuple().getLongitude());

    HashMap<String, Double> values = Maps.newHashMap();
    values.put(Constants.LABEL, labelSimilarity);
    values.put(Constants.GEO, geoSimilarity);

    triplet.setSimilarity(mode.compute(values));

    return triplet;
  }

  private Double getGeoSimilarity(Double latLeft, Double lonLeft, Double latRight, Double lonRight) {
    if (Utils.isValidGeoObject(latLeft, lonLeft)
        && Utils.isValidGeoObject(latRight, lonRight)) {
      Double distance = GeoDistance.distance(latLeft, lonLeft, latRight, lonRight);

      if (distance >= Constants.MAXIMAL_GEO_DISTANCE) {
        return 0D;
      } else {
        double tmp = 1D - (distance / Constants.MAXIMAL_GEO_DISTANCE);
        BigDecimal tmpResult = new BigDecimal(tmp);

        return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
      }
    } else {
      return null;
    }
  }

  private Double getLabelSimilarity(String left, String right) {
    Preconditions.checkNotNull(left);
    Preconditions.checkNotNull(right);

    double similarity = Utils.getTrigramMetricAndSimplifyStrings()
        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());
    BigDecimal tmpResult = new BigDecimal(similarity);

    return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
}
