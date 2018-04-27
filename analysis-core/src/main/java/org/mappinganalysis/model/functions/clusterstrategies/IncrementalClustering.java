package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.config.IncrementalConfig;

import java.util.List;

public class IncrementalClustering
    implements GraphAlgorithm<Long, ObjectMap, NullValue, DataSet<Vertex<Long, ObjectMap>>> {

  private IncrementalClusteringFunction function;
  private IncrementalConfig config;

  IncrementalClustering(IncrementalClusteringFunction function) {
    this.function = function;
  }

  IncrementalClustering(IncrementalClusteringFunction function, IncrementalConfig config) {
    this.function = function;
    this.config = config;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(Graph<Long, ObjectMap, NullValue> graph)
      throws Exception {
    return graph.run(function);
  }

  /**
   * Used for building a IncrementalClustering instance.
   */
  public static final class IncrementalClusteringBuilder {
    private IncrementalClusteringStrategy clusteringStrategy = null;
    private DataSet<Vertex<Long, ObjectMap>> newElements;
    private String part;
    private IncrementalConfig config = null;

    /**
     * Default constructor
     */
    public IncrementalClusteringBuilder() {
    }

    public IncrementalClusteringBuilder(DataDomain domain,  ExecutionEnvironment env) {
      this.config.setDomain(domain);
      this.config.setExecutionEnvironment(env);
    }

    public IncrementalClusteringBuilder(IncrementalConfig config) {
      this.config = config;
    }

    public IncrementalClusteringBuilder setConfig(
        IncrementalConfig config) {
      this.config = config;

      return this;
    }

    public IncrementalClusteringBuilder setStrategy(
        IncrementalClusteringStrategy strategy) {
      this.config.setStrategy(strategy);
      this.clusteringStrategy = strategy; // remove

      return this;
    }

    /**
     * Set of data sources as string values.
     * @param sources set of string values
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setDataSources(
        List<String> sources) {
      // TODO check, perhaps only size is needed in general?
      this.config.setExistingSourcesCount(sources.size());

      return this;
    }

    /**
     * Specify new set of elements for incremental clustering
     * @param newElements set of clusters
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setMatchElements(
        DataSet<Vertex<Long, ObjectMap>> newElements) {
      this.newElements = newElements;

      return this;
    }

    /**
     * Specify source to match.
     * @param source string
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setNewSource(
        String source) {
      this.config.setNewSource(source);

      return this;
    }

    /**
     * Set blocking strategy for incremental clustering. if not set: Standard blocking
     * @param blockingStrategy blocking strategy
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setBlockingStrategy(
        BlockingStrategy blockingStrategy) {
      this.config.setBlockingStrategy(blockingStrategy);

      return this;
    }

    /**
     * Provide metric to use for similarity comparison.
     * @param metric similarity metric
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setMetric(String metric) {
      this.config.setMetric(metric);

      return this;
    }

    // used for split incremental
    public IncrementalClusteringBuilder setPart(String part) {
      this.part = part;

      return this;
    }

    /**
     * Set execution environment.
     * @param env execution environment
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setEnvironment(ExecutionEnvironment env) {
      this.config.setExecutionEnvironment(env);

      return this;
    }

    /**
     * Creates incremental clustering based on the configured parameters.
     * @return instance of incremental clustering
     */
    public IncrementalClustering build() {
      if (clusteringStrategy == null) {
        clusteringStrategy = config.getStrategy();
      }
      if (config.getExecutionEnvironment() != null) {
        if (clusteringStrategy == IncrementalClusteringStrategy.MULTI) {
          return new MultiIncrementalClustering(config);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.FIXED_SEQUENCE) {
          return new FixedIncrementalClustering(config); // basic test clusteringStrategy
        } else if (clusteringStrategy == IncrementalClusteringStrategy.BIG) {
          return new BigIncrementalClustering(config);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.SPLIT_SETTING) {
          return new SplitIncrementalClustering(config, part);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.SINGLE_SETTING) {
          return new SingleSourceIncrementalClustering(newElements, config);
        } else {
          throw new IllegalArgumentException("Unsupported clusteringStrategy: " + clusteringStrategy);
        }
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }
  }
}
