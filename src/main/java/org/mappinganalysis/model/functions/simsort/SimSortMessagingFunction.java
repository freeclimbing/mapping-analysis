package org.mappinganalysis.model.functions.simsort;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class SimSortMessagingFunction extends MessagingFunction<Long, ObjectMap, AggSimValueTuple, ObjectMap> {
  @Override
  public void sendMessages(Vertex<Long, ObjectMap> vertex) throws Exception {
    boolean isVertexActive = !vertex.getValue().containsKey(Utils.VERTEX_STATUS)
        || (boolean) vertex.getValue().get(Utils.VERTEX_STATUS);

    if (isVertexActive) {
      for (Edge<Long, ObjectMap> edge : getEdges()) {
        AggSimValueTuple message = new AggSimValueTuple(
            (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE),
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
