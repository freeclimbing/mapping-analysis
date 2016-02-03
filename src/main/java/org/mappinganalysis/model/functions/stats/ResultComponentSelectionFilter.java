package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.List;

public class ResultComponentSelectionFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(ResultComponentSelectionFilter.class);

  private final List<Long> clusterList;

  public ResultComponentSelectionFilter(List<Long> clusterList) {
    this.clusterList = clusterList;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (vertex.getValue().containsKey(Utils.CC_ID)
        && clusterList.contains((long) vertex.getValue().get(Utils.CC_ID))) {
      LOG.info(Utils.toLog(vertex));
      return true;
    } else {
      return false;
    }
  }
}
