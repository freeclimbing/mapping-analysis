package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class StatisticsClusterCounterRichMapFunction
    extends RichMapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
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

  public StatisticsClusterCounterRichMapFunction(String stage) {
    this.stage = stage;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_ONE_ACCUMULATOR), counterOne);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_TWO_ACCUMULATOR), counterTwo);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_THREE_ACCUMULATOR), counterThree);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_FOUR_ACCUMULATOR), counterFour);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_FIVE_ACCUMULATOR), counterFive);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_SIX_ACCUMULATOR), counterSix);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_SEVEN_ACCUMULATOR), counterSeven);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_EIGHT_ACCUMULATOR), counterEight);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_NINE_ACCUMULATOR), counterNine);
    getRuntimeContext().addAccumulator(stage.concat(Constants.CLUSTER_SIZE_TEN_ACCUMULATOR), counterTen);
  }

  @Override
  public Vertex<Long, ObjectMap> map(
      Vertex<Long, ObjectMap> vertex) throws Exception {
    switch (vertex.getValue().getVerticesCount()) {
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
      case 9:
        counterNine.add(1L);
        break;
      case 10:
        counterTen.add(1L);
        break;
      default:
        throw new IllegalArgumentException();
    }

    return vertex;
  }
}
