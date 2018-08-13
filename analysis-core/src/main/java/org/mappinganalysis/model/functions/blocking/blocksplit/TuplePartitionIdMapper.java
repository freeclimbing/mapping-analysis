package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.mappinganalysis.model.MergeTuple;

/**
 * Create reduced tuples for max both computation.
 */
@FunctionAnnotation.ForwardedFields({"f0","f10->f1"})
class TuplePartitionIdMapper
    extends RichMapFunction<MergeTuple, Tuple3<Long, String, Integer>> {

  /**
   * Input music tuple is transformed for block split.
   * @param tuple music tuple
   * @return Tuple3 with id, blocking label and runtime index of this subtask
   */
  @Override
  public Tuple3<Long, String, Integer> map(MergeTuple tuple) throws Exception {
    return Tuple3.of(
        tuple.getId(),
        tuple.getBlockingLabel(),
        getRuntimeContext().getIndexOfThisSubtask());
  }
}
