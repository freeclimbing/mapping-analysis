package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;

import java.util.HashSet;

/**
 * Create base triplets for merge process, actual properties are added after checking restrictions
 */
class BaseTripletCreateFunction
    implements GroupReduceFunction<MergeTuple, Triplet<Long, ObjectMap, NullValue>> {
  private final Triplet<Long, ObjectMap, NullValue> reuseTriplet;
  private final int sourcesCount;

  public BaseTripletCreateFunction(int sourcesCount) {
    this.sourcesCount = sourcesCount;
    this.reuseTriplet = new Triplet<>();
  }

  @Override
  public void reduce(Iterable<MergeTuple> values,
                     Collector<Triplet<Long, ObjectMap, NullValue>> out) throws Exception {
    HashSet<MergeTuple> leftSide = Sets.newHashSet(values);
    HashSet<MergeTuple> rightSide = Sets.newHashSet(leftSide);

    for (MergeTuple leftTuple : leftSide) {
      Integer leftSources = leftTuple.getIntSources();
      Integer leftTypes = leftTuple.getIntTypes();
      reuseTriplet.f0 = leftTuple.getVertexId();

      rightSide.remove(leftTuple);
      for (MergeTuple rightTuple : rightSide) {
        int summedSources = AbstractionUtils.getSourceCount(leftSources)
            + AbstractionUtils.getSourceCount(rightTuple.getIntSources());

        if (summedSources <= sourcesCount
            && !AbstractionUtils.hasOverlap(leftSources, rightTuple.getIntSources())
            && AbstractionUtils.hasOverlap(leftTypes, rightTuple.getIntTypes())) {
          reuseTriplet.f1 = rightTuple.getVertexId();
          reuseTriplet.f4 = NullValue.getInstance();
//            LOG.info("###MERGE TRIPLE: " + reuseTriplet.toString());

          out.collect(reuseTriplet);
        }
      }
    }
  }
}
