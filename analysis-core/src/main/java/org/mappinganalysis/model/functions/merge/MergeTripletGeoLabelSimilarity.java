package org.mappinganalysis.model.functions.merge;

import com.google.common.base.Preconditions;
import com.sun.tools.internal.jxc.ap.Const;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeEdge;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Add similarities to Merge Triplets based on property values.
 */
class MergeTripletGeoLabelSimilarity
    extends SimilarityFunction<MergeTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeTripletGeoLabelSimilarity.class);
  AggregationMode<MergeTriplet> mode;

  public MergeTripletGeoLabelSimilarity(MeanAggregationMode mode) {
    // todo implement mode
    this.mode = mode;
  }

  // TODO add min sim check + agg mode

  @Override
  public MergeTriplet map(MergeTriplet triplet) throws Exception {
    Double labelSimilarity = getLabelSimilarity(triplet.getSrcTuple().getLabel(),
        triplet.getTrgTuple().getLabel());

    Double geoSimilarity = getGeoSimilarity(triplet.getSrcTuple().getLatitude(),
        triplet.getSrcTuple().getLongitude(),
        triplet.getTrgTuple().getLatitude(),
        triplet.getTrgTuple().getLongitude());

//    if (triplet.getSrcId() == 42L && triplet.getTrgId() == 1010272L) {
//      LOG.info(triplet.getSrcTuple().getLabel() + " xxx " + triplet.getTrgTuple().getLabel());
//      LOG.info(triplet.getSrcId() + " " + triplet.getTrgId() + " " + labelSimilarity + " " + geoSimilarity);
//    }

    if (geoSimilarity != null) {
      triplet.setSimilarity((geoSimilarity + labelSimilarity) / 2);
    } else {
      triplet.setSimilarity(labelSimilarity);
    }

    return triplet;
  }

  private Double getGeoSimilarity(Double latLeft, Double lonLeft, Double latRight, Double lonRight) {
    if (Utils.isValidGeoObject(latLeft, lonLeft)
        && Utils.isValidGeoObject(latRight, lonRight)) {
      Double distance = GeoDistance.distance(latLeft, lonLeft, latRight, lonRight);

      if (distance >= Constants.MAXIMAL_GEO_DISTANCE) {
        return 0D;
      } else {
        BigDecimal tmpResult = null;
        double tmp = 1D - (distance / Constants.MAXIMAL_GEO_DISTANCE);
        tmpResult = new BigDecimal(tmp);

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
