package org.mappinganalysis.model.functions.simcomputation.ops;

import com.google.common.collect.Sets;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.api.CustomOperation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.util.HashSet;

/**
 * Compute similarity for a single property.
 */
public class SinglePropertySimilarity implements CustomOperation<EdgeObjectMapTriplet> {
  private EdgeObjectMapTriplet triplet;
  private String property;

  static final HashSet<String> LANGUAGES;
  static {
    LANGUAGES = Sets.newHashSet();
    LANGUAGES.add(Constants.GE);
    LANGUAGES.add(Constants.EN);
    LANGUAGES.add(Constants.SP);
    LANGUAGES.add(Constants.IT);
    LANGUAGES.add(Constants.FR);
    LANGUAGES.add(Constants.LA);
    LANGUAGES.add(Constants.HU);
    LANGUAGES.add(Constants.PO);
    LANGUAGES.add(Constants.CH);
    LANGUAGES.add(Constants.CA);
    LANGUAGES.add(Constants.GR);
    LANGUAGES.add(Constants.NO);
    LANGUAGES.add(Constants.ES);
    LANGUAGES.add(Constants.POR);
    LANGUAGES.add(Constants.FI);
    LANGUAGES.add(Constants.JA);
    LANGUAGES.add(Constants.SW);
    LANGUAGES.add(Constants.DU);
    LANGUAGES.add(Constants.RU);
    LANGUAGES.add(Constants.TU);
    LANGUAGES.add(Constants.DA);
  }

  public SinglePropertySimilarity(String property) {
    this.property = property;
  }

  @Override
  public void setInput(EdgeObjectMapTriplet inputData) {
    this.triplet = inputData;
  }

  @Override
  public EdgeObjectMapTriplet createResult() {
    switch (property) {
      case Constants.LANGUAGE:
        return handleLanguage();
      case Constants.LABEL:
        return handleLabel();
      case Constants.ARTIST:
        return handleArtist();
      case Constants.ALBUM:
        return handleAlbum();
      case Constants.YEAR:
        return handleYear();
      case Constants.LENGTH:
        return handleLength();
      case Constants.NUMBER:
        return handleNumber();
      default:
        return triplet;
    }


  }

  private EdgeObjectMapTriplet handleNumber() {
    String srcNumber = triplet.getSrcVertex().getValue().getNumber();
    String trgNumber = triplet.getTrgVertex().getValue().getNumber();

    if (srcNumber.equals(trgNumber)) {
      triplet.getEdge().getValue().setLanguageSimilarity(1D);
    }
    return triplet;
  }

  /**
   * Compute year similarity. +-1 year is handled as 0.5, exact match is 1.
   */
  private EdgeObjectMapTriplet handleYear() {
    Integer srcYear = triplet.getSrcVertex().getValue().getYear();
    Integer trgYear = triplet.getTrgVertex().getValue().getYear();

    if (srcYear == Constants.EMPTY_INT || trgYear == Constants.EMPTY_INT) {
      return triplet;
    }

    int diff = srcYear - trgYear;
    if (diff == 1 || diff == -1) {
      triplet.getEdge().getValue().setYearSimilarity(0.5D);
    } else if (diff == 0) {
      triplet.getEdge().getValue().setYearSimilarity(1D);
    } else {
      triplet.getEdge().getValue().setYearSimilarity(0D);
    }
    return triplet;
  }

  /**
   * Compute length similarity. +-1 second is handled as 0.5, exact match is 1.
   */
  private EdgeObjectMapTriplet handleLength() {
    Integer srcLength = triplet.getSrcVertex().getValue().getLength();
    Integer trgLength = triplet.getTrgVertex().getValue().getLength();

    if (srcLength == Constants.EMPTY_INT || trgLength == Constants.EMPTY_INT) {
      return triplet;
    }

    int diff = srcLength - trgLength;
    if (diff == 1 || diff == -1) {
      triplet.getEdge().getValue().setLengthSimilarity(0.5D);
    } else if (diff == 0) {
      triplet.getEdge().getValue().setLengthSimilarity(1D);
    } else {
      triplet.getEdge().getValue().setLengthSimilarity(0D);
    }
    return triplet;

  }

  /**
   * Temporary solution, migrate to extra classes.
   */
  private EdgeObjectMapTriplet handleAlbum() {
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();
    String srcAlbum = triplet.getSrcVertex().getValue().getAlbum();
    String trgAlbum = triplet.getTrgVertex().getValue().getAlbum();

    if (!srcAlbum.equals(Constants.NO_VALUE) && !trgAlbum.equals(Constants.NO_VALUE)) {
      triplet.getEdge().getValue().put(Constants.SIM_ALBUM,
          Utils.computeWithMetric(metric, srcAlbum, trgAlbum));
    }
    return triplet;
  }

  private EdgeObjectMapTriplet handleArtist() {
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();
    String srcArtist = triplet.getSrcVertex().getValue().getArtist();
    String trgArtist = triplet.getTrgVertex().getValue().getArtist();

    if (!srcArtist.equals(Constants.NO_VALUE) && !trgArtist.equals(Constants.NO_VALUE)) {
      triplet.getEdge().getValue().put(Constants.SIM_ARTIST,
          Utils.computeWithMetric(metric, srcArtist, trgArtist));
    }
    return triplet;
  }

  private EdgeObjectMapTriplet handleLabel() {
    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();
    String srcLabel = triplet.getSrcVertex().getValue().getLabel();
    String trgLabel = triplet.getTrgVertex().getValue().getLabel();

    if (!srcLabel.equals(Constants.NO_LABEL_FOUND) && !trgLabel.equals(Constants.NO_LABEL_FOUND)) {
      triplet.getEdge().getValue().put(Constants.SIM_LABEL,
          Utils.computeWithMetric(metric, srcLabel, trgLabel));
    }
    return triplet;
  }


  /**
   * DE - RU --> 0
   * EN - EN --> 1
   * EN - UNknown --> no sim
   * UNknown - UNknown --> no sim
   */
  private EdgeObjectMapTriplet handleLanguage() {
    String srcLan = triplet.getSrcVertex().getValue().getLanguage();
    String trgLan = triplet.getTrgVertex().getValue().getLanguage();

    if (srcLan.equals(trgLan)) {
      triplet.getEdge().getValue().setLanguageSimilarity(1D);
      return triplet;
    } else if (LANGUAGES.contains(srcLan)
        && LANGUAGES.contains(trgLan)) {
      triplet.getEdge().getValue().setLanguageSimilarity(0D);
    } else if (srcLan.equals(Constants.NO_OR_MINOR_LANG)
        || srcLan.equals(Constants.MU)
        || srcLan.equals(Constants.UN)
        || trgLan.equals(Constants.NO_OR_MINOR_LANG)
        || trgLan.equals(Constants.MU)
        || trgLan.equals(Constants.UN)
        ) {
      // no guessing if unknown or similar values
      return triplet;
    } else {
      /**
       * {@value #Constants.NO_VALUE} no guessing if no values
       */
      return triplet;
    }
    return triplet;
  }
}
