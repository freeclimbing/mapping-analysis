package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.mappinganalysis.model.AggSimValueTuple;

class SimSortMessageCombiner extends MessageCombiner<Long, AggSimValueTuple> {
  @Override
  public void combineMessages(MessageIterator<AggSimValueTuple> aggSimValueTuples) throws Exception {
  }
}
