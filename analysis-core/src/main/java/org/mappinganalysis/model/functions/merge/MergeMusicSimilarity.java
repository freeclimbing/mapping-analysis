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
 * Add restrictions if only 1 similarity is available. TODO look MeanAggregationMode
 */
public class MergeMusicSimilarity
    extends SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeMusicSimilarity.class);
  private MeanAggregationFunction aggregationFunction;
  private String metric;

  private MergeMusicSimilarity(String metric, MeanAggregationFunction aggregationFunction) {
    this.metric = metric;
    this.aggregationFunction = aggregationFunction;
  }

  public MergeMusicSimilarity(String metric) {
    this(metric, new MeanAggregationFunction());
  }

  // TODO add min sim check
  // TODO really only these 2 sims?
  @Override
  public MergeMusicTriplet map(MergeMusicTriplet triplet) throws Exception {
//    Double labelSimilarity = getAttributeSimilarity(Constants.LABEL, triplet);
//    Double artistSimilarity = getAttributeSimilarity(Constants.ARTIST, triplet);
//    Double albumSimilarity = getAttributeSimilarity(Constants.ALBUM, triplet);

    Double artistLabelAlbumSim = getAttributeSimilarity(Constants.ARTIST_TITLE_ALBUM, triplet);
    Double yearSim = getAttributeSimilarity(Constants.YEAR, triplet);
    Double lengthSim = getAttributeSimilarity(Constants.LENGTH, triplet);
    Double numberSim = getAttributeSimilarity(Constants.NUMBER, triplet);
//    Double languageSim = getAttributeSimilarity(Constants.LANGUAGE, triplet);

    ObjectMap values = new ObjectMap(Constants.MUSIC);

    if (artistLabelAlbumSim != null) {
      values.put(Constants.SIM_ARTIST_LABEL_ALBUM, artistLabelAlbumSim);
    }

//    if (labelSimilarity != null) {
//      values.put(Constants.SIM_LABEL, labelSimilarity);
//    }
//    if (artistSimilarity != null) {
//      values.put(Constants.SIM_ARTIST, artistSimilarity);
//    }
//    if (albumSimilarity != null) {
//      values.put(Constants.SIM_ALBUM, albumSimilarity);
//    }

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

//    LOG.info("music: " + values.toString());

    triplet.setSimilarity(values
        .runOperation(aggregationFunction)
        .getEdgeSimilarity());

    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeMusicTriplet triplet) {
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