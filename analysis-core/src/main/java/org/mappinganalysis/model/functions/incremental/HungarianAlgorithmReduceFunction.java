package org.mappinganalysis.model.functions.incremental;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.HungarianAlgorithm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Transform triplets to simple double weights and determine assignment according to
 * hungarian algorithm. Afterwards, create new set of triplets with the resulting edges.
 */
public class HungarianAlgorithmReduceFunction
    implements GroupReduceFunction<
    Triplet<Long, ObjectMap, ObjectMap>,
    Triplet<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(HungarianAlgorithmReduceFunction.class);

  @Override
  public void reduce(Iterable<Triplet<Long, ObjectMap, ObjectMap>> values,
                     Collector<Triplet<Long, ObjectMap, ObjectMap>> out) throws Exception {
    HashSet<Triplet<Long, ObjectMap, ObjectMap>> triplets = Sets.newHashSet(values);
    // if only one triplet is there, no hungarian needed
    if (triplets.size() == 1) {
      out.collect(triplets.iterator().next());

      return;
    }

    double[][] tmpWeights = new double[triplets.size()][triplets.size()];
    for (double[] weight : tmpWeights) {
      Arrays.fill(weight, 1.0);
    }
    HashMap<Long, Integer> leftSource = Maps.newHashMapWithExpectedSize(triplets.size());
    HashMap<Long, Integer> rightSource = Maps.newHashMapWithExpectedSize(triplets.size());
    int leftCounter = 0;
    int rightCounter = 0;

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : triplets) {
//      LOG.info("inTrip: " + triplet.toString());
      int leftThisRound;
      int rightThisRound;

      if (!leftSource.containsKey(triplet.getSrcVertex().getId())) {
        leftSource.put(triplet.getSrcVertex().getId(), leftCounter);
        leftThisRound = leftCounter;
        ++leftCounter;
      } else {
        leftThisRound = leftSource.get(triplet.getSrcVertex().getId());
      }
      if (!rightSource.containsKey(triplet.getTrgVertex().getId())) {
        rightSource.put(triplet.getTrgVertex().getId(), rightCounter);
        rightThisRound = rightCounter;
        ++rightCounter;
      } else {
        rightThisRound = rightSource.get(triplet.getTrgVertex().getId());
      }

      tmpWeights[leftThisRound][rightThisRound]
          = 1 - triplet.getEdge().getValue().getEdgeSimilarity();
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

    /*
     iterate over all matrix elements and assign original ids
      */
    HashMap<Long, Long> resultMap = Maps.newHashMap();
    for (int leftPos = 0; leftPos < matrixResult.length; leftPos++) {
      Long key = (long) MapUtils.invertMap(leftSource).get(leftPos);

      if (matrixResult[leftPos] != -1) {
        Long value = (long) MapUtils.invertMap(rightSource)
            .get(matrixResult[leftPos]);
        // remove processed elements, in the end only not matched elements remain
        rightSource.remove(value);
        leftSource.remove(key);
        resultMap.put(key, value);
      }
    }

//    for (Map.Entry<Long, Long> longLongEntry : resultMap.entrySet()) {
//      LOG.info("resultmap: " + longLongEntry);
//    }

    HashSet<Long> checkSet = Sets.newHashSet();
    HashSet<Triplet<Long, ObjectMap, ObjectMap>> secondRound = Sets.newHashSet();

    /*
    First iteration on triplets, only if src and target are in result, its collected.
     */
    for (Triplet<Long, ObjectMap, ObjectMap> triplet : triplets) {
      long srcId = triplet.getSrcVertex().getId();
      long trgId = triplet.getTrgVertex().getId();

      // triplet is in result, can be collected.
      // elements are added to check set, do not collect them again.
      if (resultMap.get(srcId) != null && resultMap.get(srcId) == trgId) {
//        LOG.info("(1) triplet in result: " + triplet.toString());
        checkSet.add(srcId);
        checkSet.add(trgId);

        out.collect(triplet);
      } else {
//        LOG.info("(1a) added for second round: " + triplet.toString());
        secondRound.add(triplet);
      }
    }

    for (Triplet<Long, ObjectMap, ObjectMap> triplet : secondRound) {
      long srcId = triplet.getSrcVertex().getId();
      long trgId = triplet.getTrgVertex().getId();

      if (checkSet.contains(srcId) && checkSet.contains(trgId)) {
        // don't do sth if both src and trg are already handled
      }
      // no (left side) match for vertex
      else if (leftSource.containsKey(srcId)) {
        leftSource.remove(srcId);
        ObjectMap edgeValue = triplet.getEdge().getValue();
        edgeValue.setEdgeSimilarity(1D);

        Triplet<Long, ObjectMap, ObjectMap> resultTriplet = new Triplet<>(
            triplet.getSrcVertex().getId(),
            triplet.getSrcVertex().getId(),
            triplet.getSrcVertex().getValue(),
            triplet.getSrcVertex().getValue(),
            edgeValue);

//        LOG.info("(2) no (left side) match: " + resultTriplet.toString());
        out.collect(resultTriplet);
      }
//      else if (resultMap.get(srcId) == null && !checkSet.contains(srcId)) {
//        checkSet.add(srcId);
//        ObjectMap edgeValue = triplet.getEdge().getValue();
//        edgeValue.setEdgeSimilarity(1D);
//
//        Triplet<Long, ObjectMap, ObjectMap> resultTriplet = new Triplet<>(
//            triplet.getSrcVertex().getId(),
//            triplet.getSrcVertex().getId(),
//            triplet.getSrcVertex().getValue(),
//            triplet.getSrcVertex().getValue(),
//            edgeValue);
//
//        LOG.info("(2) no (left side) match: "  +resultTriplet.toString());
//        out.collect(resultTriplet);
//      }
      // no (right side) match for vertex
      else if (rightSource.containsKey(trgId)) {
        rightSource.remove(trgId);
        ObjectMap edgeValue = triplet.getEdge().getValue();
        edgeValue.setEdgeSimilarity(1D);

        Triplet<Long, ObjectMap, ObjectMap> resultTriplet = new Triplet<>(
            triplet.getTrgVertex().getId(),
            triplet.getTrgVertex().getId(),
            triplet.getTrgVertex().getValue(),
            triplet.getTrgVertex().getValue(),
            edgeValue);

//        LOG.info("(3) no (right side) match: " + resultTriplet.toString());
        out.collect(resultTriplet);
      }
//      else { // unhandled should not occur
//        throw new IllegalStateException("Should be handled: " + triplet.toString());
//      }
    }
  }
}
