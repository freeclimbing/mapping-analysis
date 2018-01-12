package org.mappinganalysis.model.functions.incremental;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.util.HungarianAlgorithm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Create a format which can be handled by HungarianAlgorithm. Handle result and
 * create "fake" triplets for elements which do not have matching elements.
 *
 * On left as well as right side of stable marriage matrix isolated elements may occur.
 */
public class HungarianAlgorithmReduceFunction
    implements GroupReduceFunction<MergeGeoTriplet, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(HungarianAlgorithmReduceFunction.class);

  @Override
  public void reduce(Iterable<MergeGeoTriplet> values,
                     Collector<MergeGeoTriplet> out) throws Exception {
    HashSet<MergeGeoTriplet> triplets = Sets.newHashSet(values);
    // if only one triplet is there, no stable marriage needed
    if (triplets.size() == 1) {
      out.collect(triplets.iterator().next());
      return;
    }

//    if (triplets.size() == 2) {
//      MergeGeoTriplet first = triplets.iterator().next();
//      MergeGeoTriplet second = triplets.iterator().next();
//
//      if (first.getSrcId() == second.getSrcId().longValue()
//        || first.getSrcId() == second.getTrgId().longValue()
//        || first.getTrgId() == second.getSrcId().longValue()
//        || first.getTrgId() == second.getTrgId().longValue()) {
//
//        if (first.getSimilarity() > second.getSimilarity()) {
//          out.collect(first);
//        } else {
//          out.collect(second);
//        }
//        return;
//      }
//    }

    double[][] tmpWeights = new double[triplets.size()][triplets.size()];
    for (double[] weight : tmpWeights) {
      Arrays.fill(weight, 1.0);
    }
    HashMap<Long, Integer> leftSource = Maps.newHashMapWithExpectedSize(triplets.size());
    HashMap<Long, Integer> rightSource = Maps.newHashMapWithExpectedSize(triplets.size());
    int leftCounter = 0;
    int rightCounter = 0;

    for (MergeGeoTriplet triplet : triplets) {
//      if (triplet.getSrcId() == 3335L
//          || triplet.getSrcId() == 6507L
//          || triplet.getSrcId() == 2380L
//          || triplet.getTrgId() == 3335
//          || triplet.getTrgId() == 6507L
//          || triplet.getTrgId() == 2380L) {
//        LOG.info("in HA: " + triplets.size()  + " " + triplet.toString());
//      }
      int leftThisRound;
      int rightThisRound;

      if (!leftSource.containsKey(triplet.getSrcId())) {
//        LOG.info("put: " + leftCounter + " " + triplet.getSrcId());
        leftSource.put(triplet.getSrcId(), leftCounter);
        leftThisRound = leftCounter;
        ++leftCounter;
      } else {
        leftThisRound = leftSource.get(triplet.getSrcId());
      }
      if (!rightSource.containsKey(triplet.getTrgId())) {
//        LOG.info("put: " + rightCounter + " " + triplet.getTrgId());
        rightSource.put(triplet.getTrgId(), rightCounter);
        rightThisRound = rightCounter;
        ++rightCounter;
      } else {
        rightThisRound = rightSource.get(triplet.getTrgId());
      }

//      LOG.info("add [" + leftThisRound + "][" + rightThisRound + "] = "
//          + (1 - triplet.getSimilarity()));
      tmpWeights[leftThisRound][rightThisRound] = 1 - triplet.getSimilarity();
    }

    double[][] weights = new double[leftCounter][rightCounter];
    // reduce array size to max counted candidate size
    for (int leftIndex = 0; leftIndex < leftCounter; ++leftIndex) {
      System.arraycopy(tmpWeights[leftIndex], 0,
          weights[leftIndex], 0,
          rightCounter);
    }

    HungarianAlgorithm algorithm = new HungarianAlgorithm(weights);
    int[] matrixResult = algorithm.execute();

    HashMap<Long, Long> resultMap = Maps.newHashMap();
    // iterate over all matrix elements and assign original ids
    for (int leftPos = 0; leftPos < matrixResult.length; leftPos++) {
//      LOG.info("run: " + leftPos);
//      LOG.info("key: " + MapUtils.invertMap(leftSource).get(leftPos).toString());
      Long key = (long) MapUtils.invertMap(leftSource).get(leftPos);

      if (matrixResult[leftPos] != -1) {
//        LOG.info("matrix value for: " + leftPos + " is " + matrixResult[leftPos]);
//        LOG.info("value: " + MapUtils.invertMap(rightSource).get(matrixResult[leftPos]).toString());
        Long value = (long) MapUtils.invertMap(rightSource)
            .get(matrixResult[leftPos]);
        // remove processed elements, in the end only not matched elements remain
        rightSource.remove(value);
        resultMap.put(key, value);
      }
    }
//    for (Map.Entry<Long, Integer> longIntegerEntry : rightSource.entrySet()) {
//      LOG.info("right side single elements: " + longIntegerEntry.toString());
//    }

    HashSet<Long> checkSet = Sets.newHashSet();
    for (MergeGeoTriplet triplet : triplets) {
      long srcId = triplet.getSrcId();
      long trgId = triplet.getTrgId();

      if (resultMap.get(srcId) == null && !checkSet.contains(srcId)) { // no (left side) match for vertex
//        LOG.info(resultMap.get(srcId) + " SET SRC TO TRG  and collect NULLtriplet: " + triplet.toString());
        triplet.setTrgTuple(triplet.getSrcTuple());
        triplet.setTrgId(srcId);
        triplet.setSimilarity(1D);
        checkSet.add(srcId);

        out.collect(triplet);
      } else if (resultMap.get(srcId) != null && resultMap.get(srcId) == trgId) {

        out.collect(triplet);
      } else if (rightSource.containsKey(trgId)) { // no (right side) match for vertex
        rightSource.remove(trgId);
        triplet.setSrcTuple(triplet.getTrgTuple());
        triplet.setSrcId(trgId);
        triplet.setSimilarity(1D);

        out.collect(triplet);
      }
//      else { // everything else are unneeded candidates
//        LOG.info("not handled: " + triplet.toString());
//      }
    }
  }
}
