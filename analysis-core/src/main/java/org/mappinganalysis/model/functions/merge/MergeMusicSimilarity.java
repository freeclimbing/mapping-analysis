package org.mappinganalysis.model.functions.merge;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;

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
//    Double labelSimilarity = getAttributeSimilarity(Constants.LABEL, triplet);
//    Double artistSimilarity = getAttributeSimilarity(Constants.ARTIST, triplet);
//    Double albumSimilarity = getAttributeSimilarity(Constants.ALBUM, triplet);

    Double artistLabelAlbumSim = getAttributeSimilarity(Constants.ARTIST_TITLE_ALBUM, triplet);
    Double yearSim = getAttributeSimilarity(Constants.YEAR, triplet);
    Double lengthSim = getAttributeSimilarity(Constants.LENGTH, triplet);
    Double numberSim = getAttributeSimilarity(Constants.NUMBER, triplet);
    Double languageSim = getAttributeSimilarity(Constants.LANGUAGE, triplet);

    ObjectMap values = new ObjectMap();

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
    if (languageSim != null) { // TODO
      values.put(Constants.SIM_LANG, languageSim);
    }

    triplet.setSimilarity(values
        .runOperation(new MeanAggregationFunction())
        .getEdgeSimilarity());

    return triplet;
  }

  private Double getAttributeSimilarity(String attrName, MergeMusicTriplet triplet) {
    switch (attrName) {
      case Constants.ARTIST_TITLE_ALBUM:
        return handleString(Constants.LABEL, triplet);
      case Constants.LANGUAGE:
        return null;
      case Constants.LABEL:
        return handleString(Constants.LABEL, triplet);
      case Constants.ARTIST:
        return handleString(Constants.ARTIST, triplet);
      case Constants.ALBUM:
        return handleString(Constants.ALBUM, triplet);
      case Constants.YEAR:
        return handleYear(triplet);
      case Constants.LENGTH:
        return handleLength(triplet);
      case Constants.NUMBER:
        return handleNumber(triplet);
      default:
        return null;
    }
  }

  private Double handleNumber(MergeMusicTriplet triplet) {
    String srcNumber = triplet.getSrcTuple().getNumber();
    String trgNumber = triplet.getTrgTuple().getNumber();

    if (srcNumber.equals(trgNumber)) {
      return 1D;
    } else {
      return null;
    }
  }

  private Double handleLength(MergeMusicTriplet triplet) {
    Integer srcLength = triplet.getSrcTuple().getLength();
    Integer trgLength = triplet.getTrgTuple().getLength();

    if (srcLength == Constants.EMPTY_INT || trgLength == Constants.EMPTY_INT) {
      return null;
    }

    int diff = srcLength - trgLength;
    if (diff == 1 || diff == -1) {
      return 0.5D;
    } else if (diff == 0) {
      return 1D;
    } else {
      return 0D;
    }
  }

  private Double handleYear(MergeMusicTriplet triplet) {
    Integer srcYear = triplet.getSrcTuple().getYear();
    Integer trgYear = triplet.getTrgTuple().getYear();

    if (srcYear == Constants.EMPTY_INT || trgYear == Constants.EMPTY_INT) {
      return null;
    }

    int diff = srcYear - trgYear;
    if (diff == 1 || diff == -1) {
      return 0.5D;
    } else if (diff == 0) {
      return 1D;
    } else {
      return 0D;
    }
  }

  private Double handleString(String attrName, MergeMusicTriplet triplet) {
    String left = triplet.getSrcTuple().getString(attrName);
    String right = triplet.getTrgTuple().getString(attrName);

    if (!Utils.isSane(left) || !Utils.isSane(right)) {
      return null;
    }

    double similarity = Utils.getTrigramMetricAndSimplifyStrings()
        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());

    return Utils.getExactDoubleResult(similarity);
  }
}