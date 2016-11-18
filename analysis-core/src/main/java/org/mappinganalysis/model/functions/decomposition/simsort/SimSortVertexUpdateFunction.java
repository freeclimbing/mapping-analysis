package org.mappinganalysis.model.functions.decomposition.simsort;

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

@Deprecated
public class SimSortVertexUpdateFunction
    extends VertexUpdateFunction<Long, ObjectMap, AggSimValueTuple> {
  private static final Logger LOG = Logger.getLogger(SimSortVertexUpdateFunction.class);
  private final double threshold;

  public SimSortVertexUpdateFunction(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public void updateVertex(Vertex<Long, ObjectMap> vertex,
                           MessageIterator<AggSimValueTuple> inMessages) throws Exception {
    ObjectMap properties = vertex.getValue();
    // default value is -1, changes with each run
    double vertexAggSim = properties.getVertexSimilarity();

    // If vertex status is already set, vertex has already been processed.
    boolean hasNoVertexState = !properties.containsKey(Constants.VERTEX_STATUS);

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

      properties.setVertexSimilarity(iterationAggSim);

      if (Doubles.compare(vertexAggSim, Constants.DEFAULT_VERTEX_SIM) != 0
          && !isLowerSimInList(iterationAggSim, neighborList)) {
        // deactivate vertex if below threshold and no neighbor has lower sim
        if (iterationAggSim < threshold) {
          properties.put(Constants.VERTEX_STATUS, Boolean.FALSE);
          properties.put(Constants.OLD_HASH_CC, properties.get(Constants.HASH_CC));
          properties.put(Constants.HASH_CC, Utils.getHash(vertex.getId().toString() + "false"));
        } else {
          properties.put(Constants.VERTEX_STATUS, Boolean.TRUE);
        }
      }

      setNewVertexValue(properties);
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