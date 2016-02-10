package org.mappinganalysis.model.functions.simsort;

import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class SimSortVertexUpdateFunction extends VertexUpdateFunction<Long, ObjectMap, AggSimValueTuple> {
  private final double threshold;

  public SimSortVertexUpdateFunction(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex,
                           MessageIterator<AggSimValueTuple> inMessages) throws Exception {
    double vertexAggSim = (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE);

    if (Doubles.compare(vertexAggSim, Utils.DEACTIVATE_VERTEX) != 0
        || Doubles.compare(vertexAggSim, Utils.DEFAULT_VERTEX_SIM) == 0) {
      boolean isMinimumSim = true;
      double result = 0;
      long messageCount = 0;
      for (AggSimValueTuple message : inMessages) {
        ++messageCount;
        if (Doubles.compare(message.getVertexSim(), vertexAggSim) < 0) {
          isMinimumSim = false;
        }
        result += message.getEdgeSim();
      }

      if (Doubles.compare(vertexAggSim, Utils.DEFAULT_VERTEX_SIM) == 0  || !isMinimumSim) {
        vertex.getValue().put(Utils.VERTEX_AGG_SIM_VALUE, result / messageCount);
        setNewVertexValue(vertex.getValue());
      } else if (result < threshold) {
        vertex.getValue().put(Utils.VERTEX_AGG_SIM_VALUE, Utils.DEACTIVATE_VERTEX);
        setNewVertexValue(vertex.getValue());
      }
    }
  }
}
