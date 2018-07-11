package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;

public class CreatePairedVertices
    implements GroupCombineFunction<
    Tuple5<Long, String, Long, Boolean, Integer>,
    Tuple2<Long, Long>> {
  @Override
  public void combine(Iterable<Tuple5<Long, String, Long, Boolean, Integer>> input,
                      Collector<Tuple2<Long, Long>> out) throws Exception {
    Collection<Tuple2<Long, Boolean>> tuples = new ArrayList<>();
    for (Tuple5<Long, String, Long, Boolean, Integer> i : input){
      tuples.add(Tuple2.of(i.f0, i.f3));
    }

    Tuple2<Long, Boolean>[] tuplesArray = tuples.toArray(new Tuple2[tuples.size()]);
    for (int i = 0; i< tuplesArray.length && tuplesArray [i].f1; i++) {
      for (int j = i+1; j< tuplesArray.length ; j++) {
        out.collect(new Tuple2<>(tuplesArray[i].f0, tuplesArray[j].f0));
      }
    }
  }
}

