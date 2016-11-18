package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.HashSet;

/**
 * Create triplets for merge, properties are added after checking restrictions,
 * similarity is not added in creation process.
 */
class MergeTripletCreator
    implements GroupReduceFunction<MergeTuple, MergeTriplet> {
  private static final Logger LOG = Logger.getLogger(MergeTripletCreator.class);

  private final MergeTriplet reuseTriplet;
  private final int sourcesCount;

  public MergeTripletCreator(int sourcesCount) {
    this.sourcesCount = sourcesCount;
    this.reuseTriplet = new MergeTriplet();
  }

  @Override
  public void reduce(Iterable<MergeTuple> values,
                     Collector<MergeTriplet> out) throws Exception {
    HashSet<MergeTuple> leftSide = Sets.newHashSet(values);
    HashSet<MergeTuple> rightSide = Sets.newHashSet(leftSide);

    for (MergeTuple leftTuple : leftSide) {
      Integer leftSources = leftTuple.getIntSources();
      Integer leftTypes = leftTuple.getIntTypes();

      reuseTriplet.setSrcId(leftTuple.getId());
      reuseTriplet.setSrcTuple(leftTuple);
      reuseTriplet.setBlockingLabel(leftTuple.getBlockingLabel());
      rightSide.remove(leftTuple);
      for (MergeTuple rightTuple : rightSide) {
        int summedSources = AbstractionUtils.getSourceCount(leftSources)
            + AbstractionUtils.getSourceCount(rightTuple.getIntSources());

        if (summedSources <= sourcesCount
            && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())
            && AbstractionUtils.hasOverlap(leftTypes, rightTuple.getIntTypes())) {

          reuseTriplet.setTrgId(rightTuple.getId());
          reuseTriplet.setTrgTuple(rightTuple);

//          LOG.info(rightTuple.toString() + " ### " + leftTuple.toString());
//          LOG.info(reuseTriplet.toString());
          out.collect(reuseTriplet);
        }
      }
    }
  }
}
