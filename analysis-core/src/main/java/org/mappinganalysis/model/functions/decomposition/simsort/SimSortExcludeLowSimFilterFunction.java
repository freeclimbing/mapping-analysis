package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Exclude vertices where vertex status is false. If vertex status is not existing, its not filtered.
 */
@Deprecated
public class SimSortExcludeLowSimFilterFunction extends RichFilterFunction<Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SimSortExcludeLowSimFilterFunction.class);

  private final boolean wantActiveVertices;
  private LongCounter filterMatches = new LongCounter();

  public SimSortExcludeLowSimFilterFunction(boolean isActiveVerticesWanted) {
    this.wantActiveVertices = isActiveVerticesWanted;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    getRuntimeContext().addAccumulator(Constants.SIMSORT_EXCLUDE_FROM_COMPONENT_ACCUMULATOR, filterMatches);
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
//    if (!vertex.getValue().getVertexStatus()) {
//      LOG.info("checksim : " + vertex.toString());
//    }

    LOG.info(vertex.toString() + " want active verts? " + wantActiveVertices
        + " return: " + (wantActiveVertices == vertex.getValue().getVertexStatus()));
    Boolean isVertexActive = vertex.getValue().getVertexStatus();
    if (!isVertexActive && wantActiveVertices) {
      filterMatches.add(1L);
    }

    return wantActiveVertices == isVertexActive;
  }
}
