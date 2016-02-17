package org.mappinganalysis.model.functions.simsort;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.AggSimValueTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

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
    double vertexAggSim = (double) vertex.getValue().get(Utils.VERTEX_AGG_SIM_VALUE);
    boolean isVertexActive = !vertex.getValue().containsKey(Utils.VERTEX_STATUS)
        || (boolean) vertex.getValue().get(Utils.VERTEX_STATUS);

    if (isVertexActive || Doubles.compare(vertexAggSim, Utils.DEFAULT_VERTEX_SIM) == 0) {
      double iterationAggSim = 0;
      long messageCount = 0;
      List<Double> neighborList = Lists.newArrayList();

      for (AggSimValueTuple message : inMessages) {
        ++messageCount;
        neighborList.add(message.getVertexSim());
        iterationAggSim += message.getEdgeSim();
      }
      iterationAggSim /= messageCount;

      vertex.getValue().put(Utils.VERTEX_AGG_SIM_VALUE, iterationAggSim);

      if (Doubles.compare(vertexAggSim, Utils.DEFAULT_VERTEX_SIM) != 0
          && !isLowerSimInList(iterationAggSim, neighborList)
          && iterationAggSim < threshold) {
        vertex.getValue().put(Utils.VERTEX_STATUS, Boolean.FALSE);
      }

      if (iterationAggSim < threshold) { // last value is not saved on the first vertex which fulfills this criteria
        setNewVertexValue(vertex.getValue());
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
