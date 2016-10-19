package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.AggSimValueTuple;

/**
 * MessagingFunction for SimSort, optimized version with Tuple instead of String Object Map
 */
public class SimSortOptMessagingFunction
    extends MessagingFunction<Long, SimSortVertexTuple, AggSimValueTuple, SimSortEdgeTuple> {
  private static final Logger LOG = Logger.getLogger(SimSortOptMessagingFunction.class);

  @Override
  public void sendMessages(Vertex<Long, SimSortVertexTuple> vertex) throws Exception {
    if (vertex.getValue().isActive()) {
      for (Edge<Long, SimSortEdgeTuple> edge : getEdges()) {

        AggSimValueTuple message = new AggSimValueTuple(
            vertex.getValue().getSim(),
            edge.getValue().getSim());
        if (vertex.getId() == edge.getSource().longValue()) {
//          LOG.info(vertex.getId() + " Send msg " + message.toString() + " to " + edge.getTarget());
            sendMessageTo(edge.getTarget(), message);
        } else {
//          LOG.info(vertex.getId() + " Send msg " + message.toString() + " to " + edge.getSource());
            sendMessageTo(edge.getSource(), message);
        }
      }
    }
  }
}