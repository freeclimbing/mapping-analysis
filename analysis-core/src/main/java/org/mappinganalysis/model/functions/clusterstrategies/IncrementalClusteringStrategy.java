package org.mappinganalysis.model.functions.clusterstrategies;

public enum IncrementalClusteringStrategy {
  ATTRIB, // not implemented
  MULTI, // BEST, based on holistic clustering, 80% from 3 sources, then 10%, then 100% 4. source, 10% of 3
  MAXSIZE, // not implemented
  LINKS, // not implemented
  BIG, // big geo dataset
  @Deprecated
  FIXED_SEQUENCE, // cluster 4 sources in one run
  SINGLE_SETTING, // cluster each source after another
  @Deprecated
  SPLIT_SETTING // step-by-step approach, 80% from 3 sources, then 10%, then 100% 4. source, 10% of 3
}
