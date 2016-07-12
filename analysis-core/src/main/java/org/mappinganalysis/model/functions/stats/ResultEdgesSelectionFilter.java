package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Edge;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;

import java.util.List;

public class ResultEdgesSelectionFilter implements FilterFunction<Edge<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(ResultComponentSelectionFilter.class);

  private final List<Long> clusterList;

  public ResultEdgesSelectionFilter(List<Long> clusterStats) {
    this.clusterList = clusterStats;
  }

  @Override
  public boolean filter(Edge<Long, ObjectMap> edge) throws Exception {
    if (clusterList.contains(edge.getSource()) || clusterList.contains(edge.getTarget())) {
      LOG.info(edge);
      LOG.info(Utils.toLog(edge));
      return true;
    } else {
      return false;
    }
  }
}
