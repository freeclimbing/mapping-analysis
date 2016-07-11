package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

import java.util.List;

public class ResultComponentSelectionFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(ResultComponentSelectionFilter.class);

  private final List<Long> clusterList;
  private final String ccType;

  public ResultComponentSelectionFilter(List<Long> clusterList, String cc) {
    this.clusterList = clusterList;
    this.ccType = cc;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (vertex.getValue().containsKey(ccType)
        && clusterList.contains((long) vertex.getValue().get(ccType))) {
//      LOG.info(Utils.toLog(vertex));
      return true;
    } else {
      return false;
    }
  }
}
