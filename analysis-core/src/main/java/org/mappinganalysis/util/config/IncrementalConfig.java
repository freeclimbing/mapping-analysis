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

  public void setStrategy(IncrementalClusteringStrategy strategy) {
    this.put(Constants.INCREMENTAL_STRATEGY, strategy);
  }

  public IncrementalClusteringStrategy getStrategy() {
    return (IncrementalClusteringStrategy) this.get(Constants.INCREMENTAL_STRATEGY);
  }

  public void setNewSource(String newSource) {
    this.setProperty(Constants.NEW_SOURCE, newSource);
  }

  public String getNewSource() {
    return this.getProperty(Constants.NEW_SOURCE);
  }

  @Deprecated
  public void setExistingSourcesCount(int sourcesCount) {
    this.put(Constants.DATA_SOURCES_LABEL, sourcesCount);
  }

  @Deprecated
  public int getSourcesCount() {
    return (int) this.get(Constants.DATA_SOURCES_LABEL);
  }

  public void setStep(ClusteringStep step) {
    this.put(Constants.STEP, step);
  }

  public ClusteringStep getStep() {
    return (ClusteringStep) this.get(Constants.STEP);
  }

  public List<String> getSourcesList() {
    if (this.getDataDomain() == DataDomain.MUSIC) {
      return Constants.MUSIC_SOURCES;
    } else if (this.getDataDomain() == DataDomain.GEOGRAPHY) {
      return Constants.GEO_SOURCES;
    } else if (this.getDataDomain() == DataDomain.NC) {
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
