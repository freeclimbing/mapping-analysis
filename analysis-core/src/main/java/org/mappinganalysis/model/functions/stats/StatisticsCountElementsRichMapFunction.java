package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Get statistics via accumulator values.
 */
public class StatisticsCountElementsRichMapFunction<T>
    extends RichMapFunction<T, T> {
  private LongCounter counter = new LongCounter();
  private String accumulatorName;

  public StatisticsCountElementsRichMapFunction(String accumulatorName) {
    this.accumulatorName = accumulatorName;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(accumulatorName, counter);
  }

  @Override
  public T map(T element) throws Exception {
    counter.add(1L);

    return element;
  }
}
