package org.mappinganalysis.model.functions.decomposition.simsort;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.math.BigDecimal;
import java.util.List;

/**
 * VertexUpdateFunction for SimSort, optimized version with Tuple4 instead of ObjectMap.
 */
public class SimSortOptVertexUpdateFunction
    extends VertexUpdateFunction<Long, SimSortVertexTuple, AggSimValueTuple> {
  private static final Logger LOG = Logger.getLogger(SimSortOptVertexUpdateFunction.class);

  private final double threshold;
  public SimSortOptVertexUpdateFunction(Double threshold) {
    this.threshold = threshold;
  }

  @Override
  public void updateVertex(Vertex<Long, SimSortVertexTuple> vertex,
                           MessageIterator<AggSimValueTuple> inMessages) throws Exception {
    SimSortVertexTuple properties = vertex.getValue();
    // default value is -1, changes with each run
    double vertexAggSim = properties.getSim();

//    LOG.debug("Working on vertex: " + vertex.getId());
    if (properties.isActive() || Doubles.compare(vertexAggSim, Constants.DEFAULT_VERTEX_SIM) == 0) {
      double iterationAggSim = 0;
      long messageCount = 0;
      List<Double> neighborList = Lists.newArrayList();

      for (AggSimValueTuple message : inMessages) {
//        LOG.debug("Got msg: " + message.toString());
        ++messageCount;
        neighborList.add(message.getVertexSim());
        iterationAggSim += message.getEdgeSim();
      }
      BigDecimal result = new BigDecimal(iterationAggSim / messageCount);
      iterationAggSim = result.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
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
