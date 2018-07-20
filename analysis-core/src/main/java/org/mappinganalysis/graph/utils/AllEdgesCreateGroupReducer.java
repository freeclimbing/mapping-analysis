package org.mappinganalysis.graph.utils;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.mappinganalysis.util.Constants;

import java.util.HashSet;

public class AllEdgesCreateGroupReducer<T extends Tuple>
    extends RichGroupReduceFunction<T, Edge<Long, NullValue>> {
  private LongCounter counterOne = new LongCounter();
  private LongCounter counterTwo = new LongCounter();
  private LongCounter counterThree = new LongCounter();
  private LongCounter counterFour = new LongCounter();
  private LongCounter counterFive = new LongCounter();
  private LongCounter counterSix = new LongCounter();
  private LongCounter counterSeven = new LongCounter();
  private LongCounter counterEight = new LongCounter();
  private LongCounter counterNine = new LongCounter();
  private LongCounter counterTen = new LongCounter();
  private String stage;

  /**
   * Default constructor, no accumulator used.
   */
  public AllEdgesCreateGroupReducer() {
    this.stage = Constants.EMPTY_STRING;
  }

  /**
   * Constructor to enable accumulators, set a user-defined label.
   * @param stage user-defined accumulator label
   */
  public AllEdgesCreateGroupReducer(String stage) {
    this.stage = stage;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    if (!stage.isEmpty()) {
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_ONE_ACCUMULATOR), counterOne);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_TWO_ACCUMULATOR), counterTwo);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_THREE_ACCUMULATOR), counterThree);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_FOUR_ACCUMULATOR), counterFour);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_FIVE_ACCUMULATOR), counterFive);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_SIX_ACCUMULATOR), counterSix);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_SEVEN_ACCUMULATOR), counterSeven);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_EIGHT_ACCUMULATOR), counterEight);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_NINE_ACCUMULATOR), counterNine);
      getRuntimeContext().addAccumulator(
          stage.concat(Constants.CLUSTER_SIZE_TEN_ACCUMULATOR), counterTen);
    }
  }


  @Override
  public void reduce(
      Iterable<T> values,
      Collector<Edge<Long, NullValue>> out) throws Exception {
    HashSet<T> rightVertices = Sets.newHashSet(values);
    HashSet<T> leftVertices = Sets.newHashSet(rightVertices);
    int clusterSize = 0;

    for (T leftVertex : leftVertices) {
      rightVertices.remove(leftVertex);
      clusterSize++;

      for (T rightVertex : rightVertices) {
        if ((long) leftVertex.getField(0) < (long) rightVertex.getField(0)) {
          out.collect(new Edge<>(leftVertex.getField(0),
              rightVertex.getField(0),
              NullValue.getInstance()));
        } else {
          out.collect(new Edge<>(rightVertex.getField(0),
              leftVertex.getField(0),
              NullValue.getInstance()));
        }
      }
    }

    if (!stage.isEmpty()) {
      switch (clusterSize) {
        case 1:
          counterOne.add(1L);
          break;
        case 2:
          counterTwo.add(1L);
          break;
        case 3:
          counterThree.add(1L);
          break;
        case 4:
          counterFour.add(1L);
          break;
        case 5:
          counterFive.add(1L);
          break;
        case 6:
          counterSix.add(1L);
          break;
        case 7:
          counterSeven.add(1L);
          break;
        case 8:
          counterEight.add(1L);
          break;
        case 9:
          counterNine.add(1L);
          break;
        case 10:
          counterTen.add(1L);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
  }
}
