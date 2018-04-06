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
    this.setDomain(domain);
    this.setExecutionEnvironment(env);
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

  public void setDomain(DataDomain domain) {
    this.put(Constants.DATA_DOMAIN, domain);
  }

  public DataDomain getDataDomain() {
    return (DataDomain) this.get(Constants.DATA_DOMAIN);
  }

  public void setMetric(String metric) {
    if (metric != null) {
      this.put(Constants.METRIC, metric);
    }
  }

  public String getMetric() {
    return this.getProperty(Constants.METRIC);
  }
}
