package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class TypeGroupByMessagingFunction extends MessagingFunction<Long, ObjectMap, ObjectMap, ObjectMap> {
  private static final Logger LOG = Logger.getLogger(TypeGroupByMessagingFunction.class);

  @Override
  public void sendMessages(Vertex<Long, ObjectMap> vertex) throws Exception {
    for (Edge<Long, ObjectMap> edge : getEdges()) {
      vertex.getValue().put(Utils.AGGREGATED_SIM_VALUE, edge.getValue().get(Utils.AGGREGATED_SIM_VALUE));
      vertex.getValue().put(Utils.VERTEX_ID, vertex.getId());
      if ((long) vertex.getId() == edge.getSource()) {
        sendMessageTo(edge.getTarget(), vertex.getValue());
      } else {
        sendMessageTo(edge.getSource(), vertex.getValue());
      }
    }
  }
}
