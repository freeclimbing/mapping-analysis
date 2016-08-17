package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Exclude vertices where vertex status is false. If vertex status is not existing, its not filtered.
 */
public class SimSortExcludeLowSimFilterFunction extends RichFilterFunction<Vertex<Long, ObjectMap>> {
  private final boolean wantActiveVertices;
  private LongCounter filterMatches = new LongCounter();

  public SimSortExcludeLowSimFilterFunction(boolean wantActiveVertices) {
    this.wantActiveVertices = wantActiveVertices;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.SIMSORT_EXCLUDE_FROM_COMPONENT_ACCUMULATOR, filterMatches);
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    boolean isVertexStatusActive = !vertex.getValue().containsKey(Constants.VERTEX_STATUS)
        || (boolean) vertex.getValue().get(Constants.VERTEX_STATUS);
    if (isVertexStatusActive && !wantActiveVertices) {
      filterMatches.add(1L);
    }

    return wantActiveVertices == isVertexStatusActive;
  }
}
