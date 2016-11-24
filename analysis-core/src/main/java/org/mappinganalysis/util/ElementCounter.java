package org.mappinganalysis.util;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

/**
 * Count elements in a certain dataset
 */
public class ElementCounter extends RichMapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private final String name;
  private LongCounter vertexCounter = new LongCounter();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(name, vertexCounter);
  }

  public ElementCounter(String name) {
    this.name = name;
  }

  @Override
  public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> value) throws Exception {
    vertexCounter.add(1L);

    return value;
  }
}
