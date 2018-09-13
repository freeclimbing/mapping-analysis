package org.mappinganalysis.model.functions.merge;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.model.functions.simcomputation.SimCompUtils;
import org.mappinganalysis.util.Constants;

import java.io.Serializable;

/**
 * Add restrictions if only 1 similarity is available. TODO look MeanAggregationMode
 */
public class MergeMusicSimilarity
    extends SimilarityFunction<MergeTriplet, MergeTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeMusicSimilarity.class);
  private MeanAggregationFunction aggregationFunction;
  private String metric;

  /**
   * Similarity function for MergeMusicTriplets, used in (old) Merge.
   */
  private MergeMusicSimilarity(String metric, MeanAggregationFunction aggregationFunction) {
    this.metric = metric;
    this.aggregationFunction = aggregationFunction;
  }

  /**
   * Constructor for intern and test usage.
   */
  public MergeMusicSimilarity(String metric) {
    this(metric, new MeanAggregationFunction());
  }

  // TODO add min sim check
  /**
   * Compute similarities for MUSIC domain based on properties.
   * {@value Constants#ARTIST_TITLE_ALBUM} is chosen over single values due to more reliability.
   * @param triplet triplet where similarity is added to
   * @return updated triplet
   */
  @Override
  public MergeTriplet map(MergeTriplet triplet) throws Exception {
    Double artistLabelAlbumSim = getAttributeSimilarity(Constants.ARTIST_TITLE_ALBUM, triplet);
    Double yearSim = getAttributeSimilarity(Constants.YEAR, triplet);
    Double lengthSim = getAttributeSimilarity(Constants.LENGTH, triplet);
    Double numberSim = getAttributeSimilarity(Constants.NUMBER, triplet);
//    Double languageSim = getAttributeSimilarity(Constants.LANGUAGE, triplet);

    ObjectMap values = new ObjectMap(Constants.MUSIC);

    if (artistLabelAlbumSim != null) {
      values.put(Constants.SIM_ARTIST_LABEL_ALBUM, artistLabelAlbumSim);
    }
    if (yearSim != null) {
      values.put(Constants.SIM_YEAR, yearSim);
    }
    if (lengthSim != null) {
      values.put(Constants.SIM_LENGTH, lengthSim);
    }
    if (numberSim != null) {
      values.put(Constants.SIM_NUMBER, numberSim);
    }
//    if (languageSim != null) { // TODO
//      values.put(Constants.SIM_LANG, languageSim);
//    }

    triplet.setSimilarity(values
        .runOperation(aggregationFunction)
        .getEdgeSimilarity());
    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeTriplet triplet) {
    switch (attrName) {
      case Constants.ARTIST_TITLE_ALBUM:
        return SimCompUtils.handleString(Constants.LABEL, triplet, metric);
      case Constants.LANGUAGE:
        return null;
      case Constants.LABEL:
        return SimCompUtils.handleString(Constants.LABEL, triplet, metric);
      case Constants.ARTIST:
        return SimCompUtils.handleString(Constants.ARTIST, triplet, metric);
      case Constants.ALBUM:
        return SimCompUtils.handleString(Constants.ALBUM, triplet, metric);
      case Constants.YEAR:
        return SimCompUtils.handleYear(triplet);
      case Constants.LENGTH:
        return SimCompUtils.handleLength(triplet);
      case Constants.NUMBER:
        return SimCompUtils.handleNumber(triplet);
      default:
        return null;
    }
  }

}