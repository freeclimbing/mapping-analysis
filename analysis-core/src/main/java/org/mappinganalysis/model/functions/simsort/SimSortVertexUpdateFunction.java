package org.mappinganalysis.model.functions.simsort;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.math.BigDecimal;
import java.util.List;

public class SimSortVertexUpdateFunction extends VertexUpdateFunction<Long, ObjectMap, AggSimValueTuple> {
  private static final Logger LOG = Logger.getLogger(SimSortVertexUpdateFunction.class);
  private final double threshold;

  public SimSortVertexUpdateFunction(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex,
                           MessageIterator<AggSimValueTuple> inMessages) throws Exception {
    double vertexAggSim = (double) vertex.getValue().get(Constants.VERTEX_AGG_SIM_VALUE);
    boolean hasNoVertexState = !vertex.getValue().containsKey(Constants.VERTEX_STATUS);
//        || (boolean) vertex.getValue().get(Utils.VERTEX_STATUS);

    if (hasNoVertexState || Doubles.compare(vertexAggSim, Constants.DEFAULT_VERTEX_SIM) == 0) {
      double iterationAggSim = 0;
      long messageCount = 0;
      List<Double> neighborList = Lists.newArrayList();

      for (AggSimValueTuple message : inMessages) {
        ++messageCount;
        neighborList.add(message.getVertexSim());
        iterationAggSim += message.getEdgeSim();
      }
      BigDecimal result = new BigDecimal(iterationAggSim / messageCount);
      iterationAggSim = result.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();

      vertex.getValue().put(Constants.VERTEX_AGG_SIM_VALUE, iterationAggSim);

      if (Doubles.compare(vertexAggSim, Constants.DEFAULT_VERTEX_SIM) != 0
          && !isLowerSimInList(iterationAggSim, neighborList)) {
        if (iterationAggSim < threshold) {
          vertex.getValue().put(Constants.VERTEX_STATUS, Boolean.FALSE);
          vertex.getValue().put(Constants.OLD_HASH_CC, vertex.getValue().get(Constants.HASH_CC));
          vertex.getValue().put(Constants.HASH_CC, Utils.getHash(vertex.getId().toString() + "false"));
        } else {
          vertex.getValue().put(Constants.VERTEX_STATUS, Boolean.TRUE);
        }
      }

//      if (iterationAggSim < threshold) { // last value is not saved on the first vertex which fulfills this criteria
//      if ((long) vertex.getValue().get(Utils.CC_ID) == 2430L) {
//          LOG.info(" setting new value: " + vertex.getId() + "   " + iterationAggSim);
//        }
      setNewVertexValue(vertex.getValue());
//      }
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
