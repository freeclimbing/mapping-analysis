package org.mappinganalysis.model.functions.merge;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.*;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.GeoDistance;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashMap;

/**
 * Add restrictions if only 1 similarity is available. TODO look MeanAggregationMode
 */
public class MergeMusicSimilarity
    extends SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeMusicSimilarity.class);

  // TODO add min sim check
  @Override
  public MergeMusicTriplet map(MergeMusicTriplet triplet) throws Exception {
    Double labelSimilarity = getAttributeSimilarity(Constants.LABEL, triplet);
    Double artistSimilarity = getAttributeSimilarity(Constants.ARTIST, triplet);
    Double albumSimilarity = getAttributeSimilarity(Constants.ALBUM, triplet);

    ObjectMap values = new ObjectMap();

    if (labelSimilarity != null) {
      values.put(Constants.SIM_LABEL, labelSimilarity);
    }
    if (artistSimilarity != null) {
      values.put(Constants.SIM_ARTIST, artistSimilarity);
    }
    if (albumSimilarity != null) {
      values.put(Constants.SIM_ALBUM, albumSimilarity);
    }

    triplet.setSimilarity(values
        .runOperation(new MeanAggregationFunction())
        .getEdgeSimilarity());

    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeMusicTriplet triplet) {
    String left = triplet.getSrcTuple().getString(attrName);
    String right = triplet.getTrgTuple().getString(attrName);

    if (left == null
        || left.equals(Constants.NO_LABEL_FOUND)
        || left.equals(Constants.NO_VALUE)
        || left.equals(Constants.CSV_NO_VALUE)
        || right == null
        || right.equals(Constants.NO_LABEL_FOUND)
        || right.equals(Constants.NO_VALUE)
        || right.equals(Constants.CSV_NO_VALUE)) {
      return null;
    }

    double similarity = Utils.getTrigramMetricAndSimplifyStrings()
        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());

    BigDecimal result = new BigDecimal(similarity);

    double v = result.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
    LOG.info("left: " + left + " right: " + right + " " + attrName + ": " + v);

    return v;
  }
}