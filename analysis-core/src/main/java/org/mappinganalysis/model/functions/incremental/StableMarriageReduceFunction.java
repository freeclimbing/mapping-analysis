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
import java.util.Map;

public class StableMarriageReduceFunction
    implements GroupReduceFunction<MergeGeoTriplet, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(StableMarriageReduceFunction.class);

  @Override
  public void reduce(Iterable<MergeGeoTriplet> values,
                     Collector<MergeGeoTriplet> out) throws Exception {
    HashSet<MergeGeoTriplet> triplets = Sets.newHashSet(values);
    if (triplets.size() == 1) {
      out.collect(triplets.iterator().next());
      return;
    }
    LOG.info("size: " + triplets.size());
    double[][] tmpWeights = new double[triplets.size()][triplets.size()];
    for (double[] weight : tmpWeights) {
      Arrays.fill(weight, 1.0);
    }

    HashMap<Long, Integer> leftSource = Maps.newHashMapWithExpectedSize(triplets.size());
    HashMap<Long, Integer> rightSource = Maps.newHashMapWithExpectedSize(triplets.size());
    int leftCounter = 0;
    int rightCounter = 0;

    for (MergeGeoTriplet triplet : triplets) {
      int leftThisRound;
      int rightThisRound;
      LOG.info("hu: " + triplet.toString());

      if (!leftSource.containsKey(triplet.getSrcId())) {
        LOG.info("put: " + leftCounter + " " + triplet.getSrcId());
        leftSource.put(triplet.getSrcId(), leftCounter);
        leftThisRound = leftCounter;
        ++leftCounter;
      } else {
        leftThisRound = leftSource.get(triplet.getSrcId());
      }
      if (!rightSource.containsKey(triplet.getTrgId())) {
        LOG.info("put: " + rightCounter + " " + triplet.getTrgId());
        rightSource.put(triplet.getTrgId(), rightCounter);
        rightThisRound = rightCounter;
        ++rightCounter;
      } else {
        rightThisRound = rightSource.get(triplet.getTrgId());
      }

      LOG.info("add [" + leftThisRound + "][" + rightThisRound + "] = "
          + (1 - triplet.getSimilarity()));
      tmpWeights[leftThisRound][rightThisRound] = 1 - triplet.getSimilarity();
    }

    double[][] weights = new double[leftCounter][rightCounter];

    for (int leftIndex = 0; leftIndex < leftCounter; ++leftIndex) {
      System.arraycopy(tmpWeights[leftIndex], 0,
          weights[leftIndex], 0,
          rightCounter);
    }

    // only log
            for (Map.Entry<Long, Integer> integerLongEntry : leftSource.entrySet()) {
              LOG.info("left:  " + integerLongEntry.toString());
            }
            for (Map.Entry<Long, Integer> integerLongEntry : rightSource.entrySet()) {
              LOG.info("right: " + integerLongEntry.toString());
            }
            for (double[] weight : weights) {
              LOG.info("row: ");
              for (double v : weight) {
                LOG.info("w:  "+ v);
              }
            }
    // log end


    HungarianAlgorithm algorithm = new HungarianAlgorithm(weights);
    int[] matrixResult = algorithm.execute();
    for (int i : matrixResult) {
      LOG.info("result matrix: " + i);
    }

    HashMap<Long, Long> resultMap = Maps.newHashMap();

    LOG.info(leftSource.toString());
    LOG.info(rightSource.toString());

    for (int leftMatrixValue = 0;
         leftMatrixValue < matrixResult.length;
         leftMatrixValue++) {
      LOG.info("run: " + leftMatrixValue);
      LOG.info(MapUtils.invertMap(leftSource).toString());
      LOG.info(MapUtils.invertMap(rightSource).toString());

      LOG.info("key: " + MapUtils.invertMap(leftSource).get(leftMatrixValue).toString());
      Long key = (long) MapUtils.invertMap(leftSource).get(leftMatrixValue);

      LOG.info("matrix value for: " + leftMatrixValue
          + " is " + matrixResult[leftMatrixValue]);
      LOG.info("value: " + MapUtils.invertMap(rightSource)
          .get(matrixResult[leftMatrixValue]).toString());
      Long value = (long) MapUtils.invertMap(rightSource)
          .get(matrixResult[leftMatrixValue]);



      resultMap.put(key, value);
    }

    for (MergeGeoTriplet triplet : triplets) {
      long srcId = triplet.getSrcId();
      long trgId = triplet.getTrgId();
      if (resultMap.get(srcId) == null) {
        LOG.info(resultMap.get(srcId) + " triplet: " + triplet.toString());
      } else if (resultMap.get(srcId) == trgId) {
        out.collect(triplet);
      }
    }
  }
}
