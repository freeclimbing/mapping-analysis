package org.mappinganalysis.model.functions.simcomputation.ops;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.api.CustomOperation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.HashSet;

/**
 * Compute similarity for a single property.
 */
public class SinglePropertySimilarity implements CustomOperation<EdgeObjectMapTriplet> {
  private EdgeObjectMapTriplet triplet;
  private String property;
  private String metric;
  private static final Logger LOG = Logger.getLogger(SinglePropertySimilarity.class);

  private static final HashSet<String> LANGUAGES;
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

  public SinglePropertySimilarity(String property, String metric) {
    this.property = property;
    this.metric = metric;
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
      case Constants.ARTIST:
      case Constants.ALBUM:
      case Constants.ARTIST_TITLE_ALBUM:
        return handleString(property);
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

  /**
   * Song number for music domain, Postcod for nc domain.
   */
  private EdgeObjectMapTriplet handleNumber() {
    String srcNumber = triplet.getSrcVertex().getValue().getNumber();
    String trgNumber = triplet.getTrgVertex().getValue().getNumber();

    if (triplet.getEdge().getValue().getMode().equals(Constants.MUSIC)) {
      if (srcNumber.equals(trgNumber)) {
        triplet.getEdge().getValue().setNumberSimilarity(1D);
      } else {
        // TODO handle?
        //LOG.info("srcPostcod: " + srcNumber + " trgPostcod: " + trgNumber);
      }
    } else if (triplet.getEdge().getValue().getMode().equals(Constants.NC)) {
      srcNumber = replaceChars(srcNumber);
      trgNumber = replaceChars(trgNumber);

      if (srcNumber.equals(trgNumber)) {
        triplet.getEdge().getValue().setNumberSimilarity(1D);
      } else {
        // TODO handle?
        //LOG.info("srcPostcod: " + srcNumber + " trgPostcod: " + trgNumber);
      }
    }
    return triplet;
  }

  /**
   * For NC domain, replace certain misspelled chars with digits.
   */
  private String replaceChars(String value) {
    value = CharMatcher.is('s').replaceFrom(value, "5");
    value = CharMatcher.anyOf("l|").replaceFrom(value, "1");
    value = CharMatcher.is('z').replaceFrom(value, "2");
    value = CharMatcher.is('o').replaceFrom(value, "0");
    value = CharMatcher.is('q').replaceFrom(value, "4");
    value = CharMatcher.is('g').replaceFrom(value, "9");

    return value;
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
   * Add similarity for String property value, compare both vertex values.
   * @param property property to be used for sim computation
   * @return updated triplet
   */
  private EdgeObjectMapTriplet handleString(String property) {
    String srcProperty = triplet.getSrcVertex().getValue().get(property).toString();
    String trgProperty = triplet.getTrgVertex().getValue().get(property).toString();

    Double similarity = Utils.getSimilarityAndSimplifyForMetric(srcProperty, trgProperty, metric);
    if (similarity != null) {
      switch (property) {
        case Constants.ARTIST_TITLE_ALBUM:
          triplet.getEdge().getValue().put(Constants.SIM_ARTIST_LABEL_ALBUM, similarity);
          break;
        case Constants.ALBUM:
          triplet.getEdge().getValue().put(Constants.SIM_ALBUM, similarity);
          break;
        case Constants.ARTIST:
          triplet.getEdge().getValue().put(Constants.SIM_ARTIST, similarity);
          break;
        case Constants.LABEL:
          triplet.getEdge().getValue().put(Constants.SIM_LABEL, similarity);
          break;
      }
    } else {
      LOG.info("SSP: " + triplet.toString());
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
    } else if (LANGUAGES.contains(srcLan)
        && LANGUAGES.contains(trgLan)) {
      triplet.getEdge().getValue().setLanguageSimilarity(0D);
    }
//    } else if (srcLan.equals(Constants.NO_OR_MINOR_LANG)
//        || srcLan.equals(Constants.MU)
//        || srcLan.equals(Constants.UN)
//        || trgLan.equals(Constants.NO_OR_MINOR_LANG)
//        || trgLan.equals(Constants.MU)
//        || trgLan.equals(Constants.UN)) {
//      // no guessing if unknown or similar values
//    } else {
//      // {@value #Constants.NO_VALUE} no guessing if no values
//    }
    return triplet;
  }
}
