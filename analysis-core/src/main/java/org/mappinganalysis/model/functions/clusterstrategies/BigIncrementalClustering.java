package org.mappinganalysis.model.functions.clusterstrategies;

import org.mappinganalysis.util.config.IncrementalConfig;

class BigIncrementalClustering extends IncrementalClustering {
  BigIncrementalClustering(IncrementalConfig config) {
    super(new BigIncrementalClusteringFunction(config));
  }
}
