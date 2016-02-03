package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.MappingAnalysisExample;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

import java.util.List;

public class ResultVerticesSelectionFilter implements FilterFunction<Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(ResultVerticesSelectionFilter.class);

  private final List<Long> clusterList;

  public ResultVerticesSelectionFilter(List<Long> clusterList) {
    this.clusterList = clusterList;
  }

  @Override
  public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (clusterList.contains(vertex.getId())) {
      LOG.info(Utils.toLog(vertex));
      return true;
    } else {
      return false;
    }
  }
}
