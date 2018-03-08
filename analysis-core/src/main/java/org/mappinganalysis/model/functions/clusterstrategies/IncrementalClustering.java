package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

import java.util.List;

public class IncrementalClustering
    implements GraphAlgorithm<Long, ObjectMap, NullValue, DataSet<Vertex<Long, ObjectMap>>> {

  private IncrementalClusteringFunction function;

  public IncrementalClustering(IncrementalClusteringFunction function) {
    this.function = function;
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
    private IncrementalClusteringStrategy clusteringStrategy;
    private ExecutionEnvironment env = null;
    private List<String> sources;
    private DataSet<Vertex<Long, ObjectMap>> newElements;
    private BlockingStrategy blockingStrategy = BlockingStrategy.STANDARD_BLOCKING;
    private String part;
    private String source;
    private String metric;

    public IncrementalClusteringBuilder setStrategy(
        IncrementalClusteringStrategy strategy) {
      this.clusteringStrategy = strategy;

      return this;
    }

    /**
     * Set of data sources as string values.
     * @param sources set of string values
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setDataSources(
        List<String> sources) {
      this.sources = sources;

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
      this.source = source;

      return this;
    }

    /**
     * Set blocking strategy for incremental clustering. if not set: Standard blocking
     * @param strategy blocking strategy
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setBlockingStrategy(
        BlockingStrategy strategy) {
      this.blockingStrategy = strategy;

      return this;
    }

    // used for split incremental
    public IncrementalClusteringBuilder setMetric(String metric) {
      this.metric = metric;

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
      this.env = env;

      return this;
    }

    /**
     * Creates incremental clustering based on the configured parameters.
     * @return instance of incremental clustering
     */
    public IncrementalClustering build() {
      if (env != null) {
        if (clusteringStrategy == IncrementalClusteringStrategy.MINSIZE) {
          return new MinSizeIncClustering(sources, metric, env);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.FIXED_SEQUENCE) {
          return new FixedIncrementalClustering(blockingStrategy, metric, env); // basic test clusteringStrategy
        } else if (clusteringStrategy == IncrementalClusteringStrategy.BIG) {
          return new BigIncrementalClustering(blockingStrategy, metric, env);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.SPLIT_SETTING) {
          return new SplitIncrementalClustering(blockingStrategy, metric, part, env);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.SINGLE_SETTING) {
          return new SingleSourceIncrementalClustering(
              blockingStrategy,
              newElements,
              metric,
              source,
              sources.size(),
              env);
        } else {
          throw new IllegalArgumentException("Unsupported clusteringStrategy: " + clusteringStrategy);
        }
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }
  }
}
