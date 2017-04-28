package org.mappinganalysis.model.functions.merge;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.math.BigDecimal;

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

    if (!Utils.isSane(left) || !Utils.isSane(right)) {
      return null;
    }

    double similarity = Utils.getTrigramMetricAndSimplifyStrings()
        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());

    return new BigDecimal(similarity)
        .setScale(6, BigDecimal.ROUND_HALF_UP)
        .doubleValue();
  }
}