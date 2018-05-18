package org.mappinganalysis.model.functions.clusterstrategies;

import org.mappinganalysis.util.config.IncrementalConfig;

@Deprecated
class FixedIncrementalClustering extends IncrementalClustering {
  FixedIncrementalClustering(IncrementalConfig config) {
    super(new FixedIncrementalClusteringFunction(config));
  }
}
