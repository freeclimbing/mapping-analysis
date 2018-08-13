package org.mappinganalysis.model.functions.simcomputation;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Geo edge similarity function.
 */
public class GeoTripletSimilarityFunction
    extends SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(GeoTripletSimilarityFunction.class);

  public GeoTripletSimilarityFunction(String metric) {
    this.metric = metric;
  }

  @Override
  public MergeMusicTriplet map(MergeMusicTriplet triplet) throws Exception {

    Double labelSimilarity = Utils.getGeoSimilarityAndSimplifyForMetric(
        triplet.getSrcTuple().getLabel(),
        triplet.getTrgTuple().getLabel(),
        metric);

    Double geoSimilarity = Utils.getGeoSimilarity(triplet.getSrcTuple().getLatitude(),
        triplet.getSrcTuple().getLongitude(),
        triplet.getTrgTuple().getLatitude(),
        triplet.getTrgTuple().getLongitude());

    if (labelSimilarity != null) {
      BigDecimal result;
      if (geoSimilarity != null) {
        result = new BigDecimal((geoSimilarity + labelSimilarity) / 2);
      } else {
        result = new BigDecimal(labelSimilarity);
      }

      triplet.setSimilarity(result
          .setScale(10, BigDecimal.ROUND_HALF_UP)
          .doubleValue());
    } else {
      throw new NullPointerException("similarity should not be null");
    }

    return triplet;
  }
}
