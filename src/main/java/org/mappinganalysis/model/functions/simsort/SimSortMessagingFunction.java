package org.mappinganalysis.model.functions.simsort;

import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class SimSortMessagingFunction extends MessagingFunction<Long, ObjectMap, AggSimValueTuple, ObjectMap> {
  @Override
  public void sendMessages(Vertex<Long, ObjectMap> vertex) throws Exception {
    double vertexAggSim = (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE);
    if (Doubles.compare(vertexAggSim, Utils.DEACTIVATE_VERTEX) != 0) {
      for (Edge<Long, ObjectMap> edge : getEdges()) {
        AggSimValueTuple message = new AggSimValueTuple(vertexAggSim,
            (double) edge.getValue().get(Utils.AGGREGATED_SIM_VALUE));
        if ((long) vertex.getId() == edge.getSource()) {
          sendMessageTo(edge.getTarget(), message);
        } else {
          sendMessageTo(edge.getSource(), message);
        }
      }
    }
  }
}