package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * Compute changes within one delta iteration step, music domain.
 */
public class ChangesMusicOperation
    implements CustomUnaryOperation<MergeMusicTriplet, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(ChangesMusicOperation.class);

  private DataSet<MergeMusicTuple> delta;
  private DataSet<Tuple2<Long, Long>> transitions;
  private DataDomain domain;
  private DataSet<MergeMusicTriplet> workset;

  ChangesMusicOperation(DataSet<MergeMusicTuple> delta,
                        DataSet<Tuple2<Long, Long>> transitions,
                        DataDomain domain) {
    this.delta = delta;
    this.transitions = transitions;
    this.domain = domain;
  }

  @Override
  public void setInput(DataSet<MergeMusicTriplet> inputData) {
    this.workset = inputData;
  }

  @Override
  public DataSet<MergeMusicTriplet> createResult() {
    DataSet<MergeMusicTriplet> leftChanges = workset.join(transitions)
        .where(0)
        .equalTo(0)
        .with(new TransitionMusicJoinFunction(0))
        .distinct(0,1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(0)
        .equalTo(0)
        .with(new TripletTupleMusicJoinFunction(0));

    DataSet<MergeMusicTriplet> notChangedLeftSide = workset.leftOuterJoin(transitions)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>());
//        .map(x -> {
//          if (x.getSrcId() == 9L && x.getTrgId() == 20L) {
//            LOG.info(x.toString());
//          }
//        });

    DataSet<MergeMusicTriplet> rightChanges = leftChanges.union(notChangedLeftSide)
//        .where(0,1)
//        .equalTo(0,1)
//        .with((left, right) -> { // leftChanges could contain elements which are changed
//          if (left == null) {    // on right side, too
//            return right;
//          } else {
//            return left;
//          }
//        })
//        .returns(new TypeHint<MergeMusicTriplet>() {})
        .join(transitions)
        .where(1)
        .equalTo(0)
        .with(new TransitionMusicJoinFunction(1))
        .distinct(0, 1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(1)
        .equalTo(0)
        .with(new TripletTupleMusicJoinFunction(1));

    DataSet<Tuple2<Long, Long>> removeFromLeftChanges = rightChanges.leftOuterJoin(transitions)
        .where(1)
        .equalTo(1)
        .with(new FlatJoinFunction<MergeMusicTriplet, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public void join(MergeMusicTriplet first, Tuple2<Long, Long> second, Collector<Tuple2<Long, Long>> out) throws Exception {
            out.collect(new Tuple2<>(first.f0, second.f0));
          }
        });

    return leftChanges.leftOuterJoin(removeFromLeftChanges)
        .where(0,1)
        .equalTo(0,1)
        .with(new LeftMinusRightSideJoinFunction<>())
        .union(rightChanges);

  }
}