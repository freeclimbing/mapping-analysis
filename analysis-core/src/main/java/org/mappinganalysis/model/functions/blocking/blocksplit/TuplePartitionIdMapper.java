package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.mappinganalysis.model.MergeMusicTuple;

//@FunctionAnnotation.ForwardedFields({"f0","f1"})

class TuplePartitionIdMapper
    extends RichMapFunction<MergeMusicTuple, Tuple3<MergeMusicTuple, String, Integer>> {

  @Override
  public Tuple3<MergeMusicTuple, String, Integer> map(MergeMusicTuple value) throws Exception {
    return Tuple3.of(value, value.getBlockingLabel(), getRuntimeContext().getIndexOfThisSubtask());
  }
}
