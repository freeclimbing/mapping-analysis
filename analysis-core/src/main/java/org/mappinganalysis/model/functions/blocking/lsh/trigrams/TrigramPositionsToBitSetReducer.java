package org.mappinganalysis.model.functions.blocking.lsh.trigrams;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.functions.blocking.lsh.structure.BloomFilter;
import org.mappinganalysis.model.functions.blocking.lsh.structure.LinkageTuple;

import java.util.BitSet;

/**
 * Tuple2 with vertex id and single (enabled) trigram bit position. Here,
 * grouped by vertex id all trigram positions are mapped to a BitSet.
 */
public class TrigramPositionsToBitSetReducer
    implements GroupReduceFunction<Tuple2<Long, Long>, LinkageTuple> {
  @Override
  public void reduce(
      Iterable<Tuple2<Long, Long>> vertexTrigramIds,
      Collector<LinkageTuple> out)
      throws Exception {
    BitSet result = new BitSet();
    Long id = null;

    for (Tuple2<Long, Long> vIdTriId : vertexTrigramIds) {
      if (id == null) {
        id = vIdTriId.f0;
      }
      result.set(vIdTriId.f1.intValue());
    }

    out.collect(new LinkageTuple(id, new BloomFilter(result)));
  }
}
