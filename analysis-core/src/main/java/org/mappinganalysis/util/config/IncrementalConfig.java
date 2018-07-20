package org.mappinganalysis.util.config;

import com.sun.tools.internal.jxc.ap.Const;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.incremental.MatchingStrategy;
import org.mappinganalysis.util.Constants;

import java.util.List;

/**
 * Config class for incremental clustering.
 */
public class IncrementalConfig extends Config {
  public IncrementalConfig(DataDomain domain, ExecutionEnvironment env) {
    super(domain, env, true);
  }

  public IncrementalConfig(IncrementalConfig config) {
    this.setBlockingStrategy(config.getBlockingStrategy());
    this.setDataDomain(config.getDataDomain());
    this.setBlockingLength(config.getBlockingLength());
  }

  public IncrementalConfig(DataDomain domain, ExecutionEnvironment env, boolean isIncremental) {
    super(domain, env, isIncremental);
  }

  /**
   * Get a config without env (and other properites).
   * TODO How to do this better?
   */
  public IncrementalConfig getConfigNoEnv() {
    return new IncrementalConfig(this);
  }


  public void setStrategy(IncrementalClusteringStrategy strategy) {
    put(Constants.INCREMENTAL_STRATEGY, strategy);
  }

  public IncrementalClusteringStrategy getStrategy() {
    return (IncrementalClusteringStrategy) get(Constants.INCREMENTAL_STRATEGY);
  }

  public void setBlockingLength(int blockingLength) {
    this.put(Constants.BLOCKING_LENGTH, blockingLength);
  }

  public int getBlockingLength() {
    if (get(Constants.BLOCKING_LENGTH) == null) {
      setBlockingLength(Constants.BLOCKING_LENGTH_DEFAULT_VALUE);
    }
    return (int) get(Constants.BLOCKING_LENGTH);
  }

  public void setNewSource(String newSource) {
    setProperty(Constants.NEW_SOURCE, newSource);
  }

  public String getNewSource() {
    return getProperty(Constants.NEW_SOURCE);
  }

  @Deprecated
  public void setExistingSourcesCount(int sourcesCount) {
    put(Constants.DATA_SOURCES_LABEL, sourcesCount);
  }

  @Deprecated
  public int getSourcesCount() {
    return (int) get(Constants.DATA_SOURCES_LABEL);
  }

  public void setStep(ClusteringStep step) {
    put(Constants.STEP, step);
  }

  public ClusteringStep getStep() {
    return (ClusteringStep) get(Constants.STEP);
  }

  public List<String> getSourcesList() {
    if (getDataDomain() == DataDomain.MUSIC) {
      return Constants.MUSIC_SOURCES;
    } else if (getDataDomain() == DataDomain.GEOGRAPHY) {
      return Constants.GEO_SOURCES;
    } else if (getDataDomain() == DataDomain.NC) {
      return Constants.NC_SOURCES;
    } else {
      return null;
    }
  }

  public void setSubGraphVerticesPath(String subGraphVerticesPath) {
    this.setProperty(Constants.SUBGRAPH_VERTICES_PATH, subGraphVerticesPath);
  }

  public String getSubGraphVerticesPath() {
    return this.getProperty(Constants.SUBGRAPH_VERTICES_PATH);
  }

  public void setMatchStrategy(MatchingStrategy matchStrategy) {
    this.put(Constants.MATCHING_STRATEGY, matchStrategy);
  }

  public MatchingStrategy getMatchStrategy() {
    return (MatchingStrategy) this.get(Constants.MATCHING_STRATEGY);
  }

  public void setMinResultSimilarity(double similarity) {
    this.put(Constants.MIN_RESULT_SIMILARITY, similarity);
  }

  public double getMinResultSimilarity() {
    return (double) this.get(Constants.MIN_RESULT_SIMILARITY);
  }
}
