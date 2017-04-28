package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;

/**
 * Create changed triplets within delta iteration operation.
 */
public class ChangesGeoOperation
    implements CustomUnaryOperation<MergeGeoTriplet, MergeGeoTriplet> {
  private DataSet<MergeGeoTuple> delta;
  private DataSet<Tuple2<Long, Long>> transitions;
  private DataDomain domain;
  private DataSet<MergeGeoTriplet> workset;

  public ChangesGeoOperation(DataSet<MergeGeoTuple> delta,
                             DataSet<Tuple2<Long, Long>> transitions,
                             DataDomain domain) {
    this.delta = delta;
    this.transitions = transitions;
    this.domain = domain;
  }

  @Override
  public void setInput(DataSet<MergeGeoTriplet> inputData) {
    this.workset = inputData;
  }

  @Override
  public DataSet<MergeGeoTriplet> createResult() {
    DataSet<MergeGeoTriplet> leftChanges = workset.join(transitions)
        .where(0)
        .equalTo(0)
        .with(new TransitionGeoJoinFunction(0))
        .distinct(0,1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(0)
        .equalTo(0)
        .with(new TripletTupleGeoJoinFunction(0));

    return workset.join(transitions)
        .where(1)
        .equalTo(0)
        .with(new TransitionGeoJoinFunction(1))
        .distinct(0,1)
        .join(delta.filter(new ActiveFilterFunction<>(domain)))
        .where(1)
        .equalTo(0)
        .with(new TripletTupleGeoJoinFunction(1))
        .union(leftChanges);
  }
}
