package org.mappinganalysis.model.functions.clusterstrategies;

import org.mappinganalysis.util.config.IncrementalConfig;

class MultiIncrementalClustering extends IncrementalClustering {
  MultiIncrementalClustering(IncrementalConfig config) {
    super(new MultiIncrementalClusteringFunction(config));
  }
}
