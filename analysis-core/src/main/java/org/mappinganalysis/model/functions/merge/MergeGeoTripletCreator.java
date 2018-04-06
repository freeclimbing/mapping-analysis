package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

import java.util.HashSet;

/**
 * Create triplets for merge, properties are added after checking restrictions,
 * similarity is not added in this creation process.
 *
 * restrictions:
 * - type of two vertices have an overlap
 * - sources of two vertices do not overlap
 * - size of resulting cluster is not bigger than numbers of data sources
 *
 * Careful:
 * - MergeTriplets have some properties which contain a start value to
 * avoid null pointer exceptions.
 * - reuse objects side effects, don't use here
 * - smaller id is always srcTuple (as long as enableSourceBasedIdSwitch is disabled)
 */
public class MergeGeoTripletCreator
    implements GroupReduceFunction<MergeGeoTuple, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(MergeGeoTripletCreator.class);
  private final int sourcesCount;
  private String newSource;
  private boolean enableSourceBasedIdSwitch;

  // (old) default behavior, switch ids and tuple if target id is bigger
  MergeGeoTripletCreator(int sourcesCount) {
    this(sourcesCount, Constants.EMPTY_STRING, false);
  }

  /**
   * Incremental Clustering behavior: only 2 sources, each source always to left/right tuple side
   * @param sourcesCount if sources count is 2, single block entities create a triplet, too
   * @param enableSourceBasedIdSwitch true if ids should be switched based on data source side
   */
  public MergeGeoTripletCreator(
      int sourcesCount,
      String newSource,
      boolean enableSourceBasedIdSwitch) {
    this.sourcesCount = sourcesCount;
    this.newSource = newSource;
    this.enableSourceBasedIdSwitch = enableSourceBasedIdSwitch;
  }

  @Override
  public void reduce(Iterable<MergeGeoTuple> values,
                     Collector<MergeGeoTriplet> out) throws Exception {
    HashSet<MergeGeoTuple> leftSide = Sets.newHashSet(values);
    HashSet<MergeGeoTuple> rightSide = Sets.newHashSet(leftSide);
    HashSet<MergeGeoTuple> checkSet = Sets.newHashSetWithExpectedSize(leftSide.size());

    for (MergeGeoTuple leftTuple : leftSide) {
      MergeGeoTriplet triplet = new MergeGeoTriplet();
      Integer leftSources = leftTuple.getIntSources();
      Integer leftTypes = leftTuple.getIntTypes();

      triplet.setBlockingLabel(leftTuple.getBlockingLabel());
      triplet.setSimilarity(0D);
      rightSide.remove(leftTuple);

      for (MergeGeoTuple rightTuple : rightSide) {
        int summedSources = AbstractionUtils.getSourceCount(leftSources)
            + AbstractionUtils.getSourceCount(rightTuple.getIntSources());
        boolean hasSrcOverlap = AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources());
        boolean hasTypeOverlap = AbstractionUtils.hasOverlap(leftTypes, rightTuple.getIntTypes());

        if (summedSources <= sourcesCount && hasTypeOverlap && !hasSrcOverlap) {
          if (enableSourceBasedIdSwitch) {
            triplet.checkSourceSwitch(leftTuple, rightTuple, newSource);
          } else {
            triplet.setIdAndTuples(leftTuple, rightTuple);
          }
          checkSet.add(triplet.getSrcTuple());
          checkSet.add(triplet.getTrgTuple());

          out.collect(triplet);
        }
      }

      /*
      if left side has only one element, no triplet is created. for 2 sources
      within incremental clustering, we still want a triplet
       */
      if (rightSide.isEmpty() && leftSide.size() == 1) {
        triplet.setIdAndTuples(leftTuple, leftTuple);

        checkSet.add(triplet.getSrcTuple());
        checkSet.add(triplet.getTrgTuple());
        out.collect(triplet);
      }

      /*
      if CURRENT leftTuple was not yet collected, we will still collect it

      more:
      if left side has only elements from one data source, no triplet is created.
      in incremental clustering, we want triplets for each element.
      */
      if (!checkSet.contains(leftTuple)) {
        triplet.setIdAndTuples(leftTuple, leftTuple);
        out.collect(triplet);
      }
    }

  }
}
