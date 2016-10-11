package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

@Deprecated
public class SimSortMessagingFunction extends MessagingFunction<Long, ObjectMap, AggSimValueTuple, ObjectMap> {
  private static final Logger LOG = Logger.getLogger(SimSortMessagingFunction.class);

  @Override
  public void sendMessages(Vertex<Long, ObjectMap> vertex) throws Exception {
    boolean hasNoVertexState = !vertex.getValue().containsKey(Constants.VERTEX_STATUS);

    if (hasNoVertexState) {
      for (Edge<Long, ObjectMap> edge : getEdges()) {
//        LOG.debug("############## " + edge.getSource() + " " + edge.getTarget() + " EDGE " + edge.toString());
//        LOG.debug("############## " + edge.getSource() + " " + edge.getTarget() + " VERTEX " + vertex.toString());
//        LOG.debug("####### " + edge.getSource() + " " + edge.getTarget() + " SIM " + Constants.VERTEX_AGG_SIM_VALUE + " "
//            + vertex.getValue().getVertexSimilarity().toString());

        AggSimValueTuple message = new AggSimValueTuple(
            vertex.getValue().getVertexSimilarity(),
            edge.getValue().getEdgeSimilarity());
        if (vertex.getId() == edge.getSource().longValue()) {
//          if (vertex.getId() < 100)
//          LOG.info("AggSim " +vertex.getId() + " send to " + edge.getTarget() + " " + message.toString());
          sendMessageTo(edge.getTarget(), message);
        } else {
//          if (vertex.getId() < 100)
//            LOG.info("AggSim " +vertex.getId() + " send to " + edge.getSource() + " " + message.toString());
          sendMessageTo(edge.getSource(), message);
        }
      }
    }
  }
}