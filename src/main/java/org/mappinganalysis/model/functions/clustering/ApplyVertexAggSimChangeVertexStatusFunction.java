package org.mappinganalysis.model.functions.clustering;

import org.apache.flink.graph.gsa.ApplyFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class ApplyVertexAggSimChangeVertexStatusFunction extends ApplyFunction<Long, ObjectMap, ObjectMap> {
  private final double threshold;

  public ApplyVertexAggSimChangeVertexStatusFunction(double threshold) {
    this.threshold = threshold;
  }

  @Override
  public void apply(ObjectMap newVal, ObjectMap currentVal) {
    double vertexSim = (double) newVal.get(Utils.AGGREGATED_SIM_VALUE)
        / (long) newVal.get(Utils.AGG_VALUE_COUNT);
    currentVal.put(Utils.VERTEX_AGG_SIM_VALUE, vertexSim);
    if (vertexSim < threshold) {
      currentVal.put(Utils.VERTEX_STATUS, Boolean.FALSE);
    }
    setResult(currentVal);
  }
}
