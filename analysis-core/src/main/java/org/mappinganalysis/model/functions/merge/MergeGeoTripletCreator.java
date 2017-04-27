package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.AbstractionUtils;

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
 * - smaller id is always srcTuple
 */
public class MergeGeoTripletCreator
    implements GroupReduceFunction<MergeGeoTuple, MergeGeoTriplet> {
  private static final Logger LOG = Logger.getLogger(MergeGeoTripletCreator.class);
  private final int sourcesCount;

  public MergeGeoTripletCreator(int sourcesCount) {
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void reduce(Iterable<MergeGeoTuple> values,
                     Collector<MergeGeoTriplet> out) throws Exception {
    HashSet<MergeGeoTuple> leftSide = Sets.newHashSet(values);
    HashSet<MergeGeoTuple> rightSide = Sets.newHashSet(leftSide);

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

        if (summedSources <= sourcesCount
            && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())
            && AbstractionUtils.hasOverlap(leftTypes, rightTuple.getIntTypes())) {

          triplet.setIdAndTuples(leftTuple, rightTuple);

          LOG.info(rightTuple.toString() + " ### " + leftTuple.toString());
          LOG.info(triplet.toString());
          out.collect(triplet);
        }
      }
    }
  }
}
