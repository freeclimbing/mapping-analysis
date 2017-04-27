package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;

/**
 * throw out everything with transition elements
 */
public class NonChangedWorksetPartOperation<T>
    implements CustomUnaryOperation<MergeGeoTriplet, MergeGeoTriplet> {
  private DataSet<MergeGeoTriplet> workset;
  private DataSet<Tuple2<Long, Long>> transitions;

  public NonChangedWorksetPartOperation(DataSet<Tuple2<Long, Long>> transitions) {
    this.transitions = transitions;
  }

  @Override
  public void setInput(DataSet<MergeGeoTriplet> inputData) {
    this.workset = inputData;
  }

  @Override
  public DataSet<MergeGeoTriplet> createResult() {
    return workset.leftOuterJoin(transitions)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(transitions)
        .where(1)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>());
  }
}
