package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.util.config.Config;
import org.mappinganalysis.util.config.IncrementalConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Check triplets for overlap in data sources and restrict
 * maximum triplet entity count based on data sources.
 */
class DataSourceOverlapCheckFilterFunction
    extends RichFilterFunction<Triplet<Long, ObjectMap, NullValue>> {
  private List<String> dataSources;
  private boolean checkSourceOverlap;

  public DataSourceOverlapCheckFilterFunction(boolean checkSourceOverlap) {
    this.checkSourceOverlap = checkSourceOverlap;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.dataSources = getRuntimeContext().getBroadcastVariable("dataSources");
  }

  @Override
  public boolean filter(Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {
    if (checkSourceOverlap) {
      Set<String> srcDataSources = triplet.getSrcVertex()
          .getValue().getDataSourcesList();
      Set<String> trgDataSources = triplet.getTrgVertex()
          .getValue().getDataSourcesList();

      int tripletElementCount = srcDataSources.size() + trgDataSources.size();

      int distinctTripletSources = Sets
          .union(srcDataSources, trgDataSources)
          .size();

      boolean hasOverlap = distinctTripletSources < tripletElementCount;

      return !hasOverlap && tripletElementCount <= dataSources.size();

    } else {
      return true;
    }
  }
}
