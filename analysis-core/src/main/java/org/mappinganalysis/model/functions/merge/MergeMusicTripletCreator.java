package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.HashSet;

/**
 * Create Triplets and do blocking.
 */
public class MergeMusicTripletCreator
    implements GroupReduceFunction<MergeTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(MergeMusicTripletCreator.class);
  private final int sourcesCount;

  public MergeMusicTripletCreator(int sourcesCount) {
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void reduce(Iterable<MergeTuple> values,
                     Collector<MergeMusicTriplet> out) throws Exception {
    HashSet<MergeTuple> leftSide = Sets.newHashSet(values);
    HashSet<MergeTuple> rightSide = Sets.newHashSet(leftSide);

    for (MergeTuple leftTuple : leftSide) {
      Integer leftSources = leftTuple.getIntSources();
      rightSide.remove(leftTuple);

      for (MergeTuple rightTuple : rightSide) {
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