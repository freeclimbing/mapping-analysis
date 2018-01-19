package org.mappinganalysis.model.functions.clusterstrategies;

public enum IncrementalClusteringStrategy {
  ATTRIB, // not implemented
  MINSIZE, // not implemented
  MAXSIZE, // not implemented
  LINKS, // not implemented
  BIG, // big geo dataset
  FIXED_SEQUENCE, // cluster 4 sources in one run
  SINGLE_SETTING, // cluster each source after another
  SPLIT_SETTING // step-by-step approach, 80% from 3 sources, then 10%, then 100% 4. source, 10% of 3
}
