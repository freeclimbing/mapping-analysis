package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.HashSet;

/**
 * Create Triplets and do blocking.
 */
public class MergeMusicTripletCreator
    implements GroupReduceFunction<MergeMusicTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(MergeMusicTripletCreator.class);
  private final int sourcesCount;

  public MergeMusicTripletCreator(int sourcesCount) {
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void reduce(Iterable<MergeMusicTuple> values,
                     Collector<MergeMusicTriplet> out) throws Exception {
    HashSet<MergeMusicTuple> leftSide = Sets.newHashSet(values);
    HashSet<MergeMusicTuple> rightSide = Sets.newHashSet(leftSide);

    for (MergeMusicTuple leftTuple : leftSide) {
      Integer leftSources = leftTuple.getIntSources();
      rightSide.remove(leftTuple);

      for (MergeMusicTuple rightTuple : rightSide) {
        int summedSources = AbstractionUtils.getSourceCount(leftSources)
            + AbstractionUtils.getSourceCount(rightTuple.getIntSources());

        if (summedSources <= sourcesCount
            && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())) {
          MergeMusicTriplet triplet = new MergeMusicTriplet(leftTuple, rightTuple);
          triplet.setBlockingLabel(leftTuple.getBlockingLabel());

//          LOG.info("MMTC 4: " + triplet.toString());
          out.collect(triplet);
        }
      }
    }
  }
}