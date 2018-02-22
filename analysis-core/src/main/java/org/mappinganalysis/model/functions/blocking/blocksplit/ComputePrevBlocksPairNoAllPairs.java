package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

public class ComputePrevBlocksPairNoAllPairs
    implements GroupReduceFunction<Tuple3<String, Long, Long>, Tuple5<String, Long, Long, Long, Long>> {
  @Override
  public void reduce(Iterable<Tuple3<String, Long, Long>> in,
                     Collector<Tuple5<String, Long, Long, Long, Long>> out) throws Exception {
    Collection<Tuple3<String, Long, Long>> key_size_index = new ArrayList<>();
    for (Tuple3<String, Long, Long> i:in){
      key_size_index.add(i);

    }

    Tuple3<String, Long, Long>[] key_size_index_Array = key_size_index.toArray(new Tuple3[key_size_index.size()]);
    key_size_index.clear();
    Arrays.sort(key_size_index_Array, new Comparator<Tuple3<String, Long, Long>>() {
      public int compare(Tuple3<String, Long, Long> in1, Tuple3<String, Long, Long> in2) {
        try {
          if (in1.f2 > in2.f2)
            return 1;
          if (in1.f2 < in2.f2)
            return -1;
          return 0;
        }
        catch (Exception e){
          System.out.println(e.getMessage());
        }
        return 0;
      }

    });

    Long cnt =0l;
    Long allPairs = 0l;
    Long[] SumPrev = new Long[key_size_index_Array.length];

    for (int i=0; i< key_size_index_Array.length; i++){
      SumPrev[i] = allPairs;
      Long size = key_size_index_Array[i].f1;
      allPairs += (size*(size-1))/2;
    }

    for (int i=0; i< key_size_index_Array.length; i++){
      out.collect(Tuple5.of( key_size_index_Array[i].f0, key_size_index_Array[i].f1, key_size_index_Array[i].f2,
          SumPrev[i],allPairs));
    }
  }
}