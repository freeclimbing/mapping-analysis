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
    LOG.info("message from: " + vertex.toString() + "ssn: " + getSuperstepNumber());
    vertex.getValue().put(Utils.VERTEX_ID, vertex.getId());

    for (Edge<Long, ObjectMap> edge : getEdges()) {
      LOG.info("processing edge: " + edge.getSource() + " -> " + edge.getTarget()
          + " " + edge.getValue().get(Utils.AGGREGATED_SIM_VALUE)
          + " on vertex: " + vertex.getId() + " ssn: " + getSuperstepNumber());
      vertex.getValue().put(Utils.AGGREGATED_SIM_VALUE, edge.getValue().get(Utils.AGGREGATED_SIM_VALUE));
      if (edge.getSource() == (long) vertex.getId()) {
        LOG.info("Send msg: " + vertex.getValue() + " to " + edge.getTarget() + " ssn: " + getSuperstepNumber());

        sendMessageTo(edge.getTarget(), vertex.getValue());
      } else {
        LOG.info("Send msg: " + vertex.getValue() + " to " + edge.getSource() + " ssn: " + getSuperstepNumber());
        sendMessageTo(edge.getSource(), vertex.getValue());
      }
    }
  }
}
