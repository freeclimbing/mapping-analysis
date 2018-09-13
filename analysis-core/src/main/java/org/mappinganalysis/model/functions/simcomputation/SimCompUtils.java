package org.mappinganalysis.model.functions.simcomputation;

import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Similarity computation util class, get double values for certain properties.
 * MUSIC, NC
 */
public class SimCompUtils {
  public static Double handleNumber(MergeTriplet triplet) {
    String srcNumber = triplet.getSrcTuple().getNumber();
    String trgNumber = triplet.getTrgTuple().getNumber();

    if (!Utils.isSane(srcNumber) || !Utils.isSane(trgNumber)) {
      return null;
    }

    if (srcNumber.equals(trgNumber)) {
      return 1D;
    } else {
      return null;
    }
  }

  public static Double handleString(String attrName, MergeTriplet triplet, String metric) {
//    System.out.println("simcomputils: " + attrName);
//    System.out.println(triplet.toString());
    String left = triplet.getSrcTuple().getString(attrName);
    String right = triplet.getTrgTuple().getString(attrName);

    return Utils.getSimilarityAndSimplifyForMetric(left, right, metric);
  }


  public static Double handleLength(MergeTriplet triplet) {
    Integer srcLength = triplet.getSrcTuple().getLength();
    Integer trgLength = triplet.getTrgTuple().getLength();

    return getDoubleSimilarity(srcLength, trgLength);
  }

  public static Double handleYear(MergeTriplet triplet) {
    Integer srcYear = triplet.getSrcTuple().getYear();
    Integer trgYear = triplet.getTrgTuple().getYear();

    return getDoubleSimilarity(srcYear, trgYear);
  }

  private static Double getDoubleSimilarity(Integer srcValue, Integer trgValue) {
    if (srcValue == Constants.EMPTY_INT || trgValue == Constants.EMPTY_INT) {
      return null;
    }

    int diff = srcValue - trgValue;
    if (diff == 1 || diff == -1) {
      return 0.5D;
    } else if (diff == 0) {
      return 1D;
    } else {
      return 0D;
    }
  }

}

