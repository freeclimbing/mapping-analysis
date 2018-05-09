package org.mappinganalysis.util.config;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Constants;

import java.util.Properties;

/**
 * Basic config class to define properties for workflow.
 */
public class Config extends Properties {

  public Config(DataDomain domain, ExecutionEnvironment env) {
    this.setDataDomain(domain);
    this.setExecutionEnvironment(env);
  }

  public Config(DataDomain domain, ExecutionEnvironment env, boolean isIncremental) {
    this.setDataDomain(domain);
    this.setExecutionEnvironment(env);
    this.setIncremental(isIncremental);
  }

  public Config(Config config) {
    this.setBlockingStrategy(config.getBlockingStrategy());
    this.setDataDomain(config.getDataDomain());
  }

  /**
   * Get a config without env (and other properites).
   * TODO How to do this better?
   */
  public Config getConfigNoEnv() {
    return new Config(this);
  }

  public void setExecutionEnvironment(ExecutionEnvironment executionEnvironment) {
    this.put(Constants.ENV, executionEnvironment);
  }

  public ExecutionEnvironment getExecutionEnvironment() {
    return (ExecutionEnvironment) this.get(Constants.ENV);
  }

  public void setBlockingStrategy(BlockingStrategy blockingStrategy) {
    this.put(Constants.BLOCKING_STRATEGY, blockingStrategy);
  }

  public BlockingStrategy getBlockingStrategy() {
    return (BlockingStrategy) this.get(Constants.BLOCKING_STRATEGY);
  }

  public void setDataDomain(DataDomain domain) {
    this.put(Constants.DATA_DOMAIN, domain);

    /*
       remove if possible, deprecated "mode"
     */
    String mode = null;
    if (domain == DataDomain.MUSIC) {
      mode = Constants.MUSIC;
    } else if (domain == DataDomain.GEOGRAPHY) {
      mode = Constants.GEO;
    } else if (domain == DataDomain.NC) {
      mode = Constants.NC;
    }
    assert mode != null;
    this.put(Constants.MODE, mode);
  }

  public DataDomain getDataDomain() {
    return (DataDomain) this.get(Constants.DATA_DOMAIN);
  }

  public String getMode() {
    return this.getProperty(Constants.MODE);
  }

  public void setMetric(String metric) {
    if (metric != null) {
      this.put(Constants.METRIC, metric);
    }
  }

  public String getMetric() {
    return this.getProperty(Constants.METRIC);
  }

  public void setSimSortSimilarity(double simSortSimilarity) {
    this.put(Constants.SIMSORT_THRESHOLD, simSortSimilarity);
  }

  public Double getSimSortSimilarity() {
    return (Double) this.get(Constants.SIMSORT_THRESHOLD);
  }

  public void setIncremental(Boolean isIncremental) {
    this.put(Constants.IS_INCREMENTAL, isIncremental);
  }

  public boolean isIncremental() {
    return (Boolean) this.get(Constants.IS_INCREMENTAL);
  }
}
