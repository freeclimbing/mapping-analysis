package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

//input: Tuple6<Vertex, String, Long, Long, Long, Long> ~ Tuple6<Vertex, Key, VIndex, BlkSize, PrevPairs, allPairs>
// output: Tuple5<Vertex, String, Long, Boolean, Integer> ~ Tuple5<Vertex, Key, VertexIndex, IsLast, ReducerId>

/**
 * input and output tuple f0 is tuple id
 */
public class ReplicateAndAssignReducerId
    extends RichFlatMapFunction<
    Tuple6<Long, String, Long, Long, Long, Long>,
    Tuple5<Long, String, Long, Boolean, Integer>> {
  private int parallelism;

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
  }

  public void flatMap(Tuple6<Long, String, Long, Long, Long, Long> input,
                      Collector<Tuple5<Long, String, Long, Boolean, Integer>> out) {
    //p (x,y) = x/2 (2 * blkSize -x-3) + y-1 + prvBlkSize
    long xMin, yMin, xMax, yMax;
    xMin = 0;
    if (input.f2 == 0) {
      xMax = 0;
      yMin = 1;
      yMax = input.f3 - 1;
    } else if (input.f2 == input.f3 - 1) {
      xMax = input.f2 - 1;
      yMin = input.f3 - 1;
      yMax = input.f3 - 1;
    } else {
      xMax = input.f2;
      yMin = input.f2;
      yMax = input.f3 - 1;
    }
    Long pMin, pMax;
    if (xMin % 2 == 0) {
      pMin = ((xMin / 2) * (2 * input.f3 - xMin - 3)) + yMin - 1 + input.f4;
    } else {
      pMin = ((xMin) * ((2 * input.f3 - xMin - 3) / 2)) + yMin - 1 + input.f4;
    }
    if (xMax % 2 == 0) {
      pMax = ((xMax / 2) * (2 * input.f3 - xMax - 3)) + yMax - 1 + input.f4;
    } else {
      pMax = ((xMax) * ((2 * input.f3 - xMax - 3) / 2)) + yMax - 1 + input.f4;
    }

    int minReducerId = Math.toIntExact(parallelism * pMin / input.f5);
    int maxReducerId = Math.toIntExact(parallelism * pMax / input.f5);

    if (minReducerId != maxReducerId)
      for (int i = minReducerId; i < maxReducerId; i++){
        out.collect(Tuple5.of(input.f0, input.f1, input.f2, false, i));
      }

    out.collect(Tuple5.of(input.f0, input.f1, input.f2, true, maxReducerId));
  }
}
