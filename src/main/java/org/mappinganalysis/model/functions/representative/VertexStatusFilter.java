package org.mappinganalysis.model.functions.representative;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Exclude vertices where vertex status is false. If vertex status is not existing, its not filtered.
 */
public class VertexStatusFilter extends RichFilterFunction<Vertex<Long, ObjectMap>> {
  private final boolean wantActiveVertices;
  private LongCounter filterMatches = new LongCounter();

  public VertexStatusFilter(boolean wantActiveVertices) {
    this.wantActiveVertices = wantActiveVertices;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Utils.EXCLUDE_FROM_COMPONENT_ACCUMULATOR, filterMatches);
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    boolean isVertexStatusActive = !vertex.getValue().containsKey(Utils.VERTEX_STATUS)
        || (boolean) vertex.getValue().get(Utils.VERTEX_STATUS);
    if (isVertexStatusActive) {
      filterMatches.add(1L);
    }

    return wantActiveVertices ? isVertexStatusActive : !isVertexStatusActive;
  }
}
