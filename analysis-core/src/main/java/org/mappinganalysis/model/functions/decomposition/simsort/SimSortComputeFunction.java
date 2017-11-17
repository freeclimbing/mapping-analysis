package org.mappinganalysis.model.functions.decomposition.simsort;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.List;

class SimSortComputeFunction
    extends ComputeFunction<Long, SimSortVertexTuple, SimSortEdgeTuple, AggSimValueTuple> {
  private static final Logger LOG = Logger.getLogger(SimSortComputeFunction.class);

  private final double threshold;
  public SimSortComputeFunction(Double threshold) {
    this.threshold = threshold;
  }

  @Override
  public void compute(
      Vertex<Long, SimSortVertexTuple> vertex,
      MessageIterator<AggSimValueTuple> inMessages) throws Exception {
//    LOG.debug("Working on vertex: " + vertex.toString());

    SimSortVertexTuple properties = vertex.getValue();
    // default value is -1, changes with each run
    double vertexAggSim = properties.getSim();

    if (Doubles.compare(vertex.getValue().getSim(), Constants.DEFAULT_VERTEX_SIM) == 0) {
      vertexAggSim = -0.5;
      properties.setSim(vertexAggSim);
      setNewVertexValue(properties);

      sendMessages(vertex);
    } else if (properties.isActive()) {
//        && Doubles.compare(vertexAggSim, Constants.DEFAULT_VERTEX_SIM) != 0) {
      double iterationAggSim = 0;
      long messageCount = 0;
      List<Double> neighborList = Lists.newArrayList();

      for (AggSimValueTuple message : inMessages) {
//        LOG.debug("Got msg: " + message.toString());
        ++messageCount;
        neighborList.add(message.getVertexSim());
        iterationAggSim += message.getEdgeSim();
      }

      iterationAggSim = Utils.getExactDoubleResult(iterationAggSim, messageCount);
//      LOG.debug(vertex.getId() + " itAggSim: " + iterationAggSim + " old: " + vertex.getValue().getSim());

      if (Doubles.compare(vertexAggSim, Constants.DEFAULT_VERTEX_SIM) != 0
          && !isLowerSimInList(iterationAggSim, neighborList)) {

//        LOG.debug("deact sequ: " +iterationAggSim + " <> " + threshold + " vertex: " + vertex.toString());
        // deactivate vertex if below threshold and no neighbor has lower sim
        if (iterationAggSim < threshold) {
//          LOG.debug("DEACT VERTEX: " + vertex.getId());
          properties.setActive(Boolean.FALSE);
          properties.setOldHash(properties.getHash());
          properties.setHash(Utils.getHash(vertex.getId().toString() + "false"));

          setNewVertexValue(properties);
        }
      }

      if (Doubles.compare(properties.getSim(), iterationAggSim) != 0 && properties.isActive()) {
        properties.setSim(iterationAggSim);
//        LOG.debug("update vertex to: " + properties.toString());
        setNewVertexValue(properties);

        sendMessages(vertex);
      }
    }
  }

  private void sendMessages(Vertex<Long, SimSortVertexTuple> vertex) {
//    LOG.info("in edge send");
    for (Edge<Long, SimSortEdgeTuple> edge : getEdges()) {
//      LOG.info("Working on edge: " + edge.toString());

      AggSimValueTuple message = new AggSimValueTuple(
          vertex.getValue().getSim(),
          edge.getValue().getSim());
      if (vertex.getId() == edge.getSource().longValue()) {
//        LOG.info(vertex.getId() + " Send msg " + message.toString() + " to " + edge.getTarget());
        sendMessageTo(edge.getTarget(), message);
      } else {
//        LOG.info(vertex.getId() + " Send msg " + message.toString() + " to " + edge.getSource());
        sendMessageTo(edge.getSource(), message);
      }
    }
  }

  public static boolean isLowerSimInList(double limit, List<Double> data){
    for (Double val : data) {
      if (Doubles.compare(val, limit) < 0) {
        return true;
      }
    }
    return false;
  }
}
