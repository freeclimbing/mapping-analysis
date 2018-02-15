package org.mappinganalysis.model.functions.merge;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.model.functions.simcomputation.SimCompUtils;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;

/**
 * compute similarities for nc dataset within merge phase based on properties
 */
public class MergeNcSimilarity
    extends SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeMusicSimilarity.class);
  private MeanAggregationFunction aggregationFunction;

  public MergeNcSimilarity() {
    this(new MeanAggregationFunction());
  }

  public MergeNcSimilarity(MeanAggregationFunction aggregationFunction) {
    this.aggregationFunction = aggregationFunction;
  }

  @Override
  public MergeMusicTriplet map(MergeMusicTriplet triplet) throws Exception {
    Double labelSimilarity = getAttributeSimilarity(Constants.LABEL, triplet);
    Double artistSimilarity = getAttributeSimilarity(Constants.ARTIST, triplet);
    Double albumSimilarity = getAttributeSimilarity(Constants.ALBUM, triplet);
    Double numberSim = getAttributeSimilarity(Constants.NUMBER, triplet);

    ObjectMap values = new ObjectMap(Constants.NC);

    if (labelSimilarity != null) {
      values.put(Constants.SIM_LABEL, labelSimilarity);
    }
    if (artistSimilarity != null) {
      values.put(Constants.SIM_ARTIST, artistSimilarity);
    }
    if (albumSimilarity != null) {
      values.put(Constants.SIM_ALBUM, albumSimilarity);
    }
    if (numberSim != null) {
      values.put(Constants.SIM_NUMBER, numberSim);
    }

//    boolean precheck = false;
//    if (Double.compare(labelSimilarity, 0.793857) == 0) {
//      LOG.info("79 " + triplet.toString());
//      LOG.info("79.. " + values.toString());
//      precheck = true;
//    }

//    LOG.info("single: " + values.toString());

    triplet.setSimilarity(values
        .runOperation(aggregationFunction)
        .getEdgeSimilarity());
//    LOG.info("aggr: " + values.toString());

//    if (precheck) {
//      LOG.info("precheck: " + values.toString());
//    }

    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeMusicTriplet triplet) {
    switch (attrName) {
      case Constants.LABEL:
        return SimCompUtils.handleString(Constants.LABEL, triplet);
      case Constants.ARTIST:
        return SimCompUtils.handleString(Constants.ARTIST, triplet);
      case Constants.ALBUM:
        return SimCompUtils.handleString(Constants.ALBUM, triplet);
      case Constants.NUMBER:
        return SimCompUtils.handleNumber(triplet);
      default:
        return null;
    }
  }
}