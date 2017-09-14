package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;

/**
 * Compute changes within one delta iteration step, music domain.
 */
public class ChangesMusicOperation
    implements CustomUnaryOperation<MergeMusicTriplet, MergeMusicTriplet> {
  private DataSet<MergeMusicTuple> delta;
  private DataSet<Tuple2<Long, Long>> transitions;
  private DataDomain domain;
  private DataSet<MergeMusicTriplet> workset;

  public ChangesMusicOperation(DataSet<MergeMusicTuple> delta,
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

    return workset.join(transitions)
        .where(1)
        .equalTo(0)
        .with(new TransitionMusicJoinFunction(1))
        .distinct(0,1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(1)
        .equalTo(0)
        .with(new TripletTupleMusicJoinFunction(1))
        .union(leftChanges);
  }
}