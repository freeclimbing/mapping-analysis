package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTuple;

//input: Tuple6<Vertex, String, Long, Long, Long, Long> ~ Tuple6<Vertex, Key, VIndex, BlkSize, PrevPairs, allPairs>
// output: Tuple5<Vertex, String, Long, Boolean, Integer> ~ Tuple5<Vertex, Key, VertexIndex, IsLast, ReducerId>

public class ReplicateAndAssignReducerId
    extends RichFlatMapFunction<Tuple6<MergeMusicTuple, String, Long, Long, Long, Long>,
    Tuple5<MergeMusicTuple, String, Long, Boolean, Integer>> {
  private int parallelism;

  ReplicateAndAssignReducerId(int parallelism) {
    this.parallelism = parallelism;
  }

  public void flatMap(Tuple6<MergeMusicTuple, String, Long, Long, Long, Long> input,
                      Collector<Tuple5<MergeMusicTuple, String, Long, Boolean, Integer>> out) {
//        getRuntimeContext().getNumberOfParallelSubtasks();
    //p (x,y) = x/2 (2 * blkSize -x-3) + y-1 + prvBlkSize
    long xmin, ymin, xmax, ymax;
    xmin = 0;
    if (input.f2 == 0) {
      xmax=0;
      ymin=1;
      ymax=input.f3 - 1;
    } else if (input.f2 == input.f3 - 1) {
      xmax=input.f2 - 1;
      ymin=input.f3 - 1;
      ymax=input.f3 - 1;
    } else {
      xmax=input.f2;
      ymin=input.f2;
      ymax=input.f3 - 1;
    }
    Long pMin, pMax;
    if (xmin % 2 == 0) {
      pMin = ((xmin / 2) * (2 * input.f3 - xmin - 3)) + ymin - 1 + input.f4;
    } else {
      pMin = ((xmin) * ((2 * input.f3 - xmin - 3) / 2)) + ymin - 1 + input.f4;
    }
    if (xmax % 2 == 0) {
      pMax = ((xmax / 2) * (2 * input.f3 - xmax - 3)) + ymax - 1 + input.f4;
    } else {
      pMax = ((xmax) * ((2 * input.f3 - xmax - 3) / 2)) + ymax - 1 + input.f4;
    }

    int minReducerId = Math.toIntExact(parallelism * pMin / input.f5);
    int maxReducerId = Math.toIntExact(parallelism * pMax / input.f5);


//        System.out.println(input.f2+","+input.f3+","+parallelism * pMin / input.f5+","+parallelism * pMax / input.f5);
    if (minReducerId != maxReducerId)
      for (int i= minReducerId; i < maxReducerId; i++){
        out.collect(Tuple5.of(input.f0, input.f1, input.f2, false, i));
      }
    out.collect(Tuple5.of(input.f0, input.f1, input.f2, true, maxReducerId));
  }
}
