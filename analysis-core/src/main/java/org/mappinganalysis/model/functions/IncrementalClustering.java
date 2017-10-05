package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.mappinganalysis.model.ObjectMap;

import java.util.List;

public class IncrementalClustering
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {

  private IncrementalClusteringFunction function;

  public IncrementalClustering(IncrementalClusteringFunction function) {
    this.function = function;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph)
      throws Exception {
    return graph.run(function);
  }

  /**
   * Used for building a link filter operator instance. On default, isolated vertices are
   * removed after filter.
   */
  public static final class IncrementalClusteringBuilder {
    private IncrementalClusteringStrategy strategy;
    private ExecutionEnvironment env = null;
    private List<String> sources;

    public IncrementalClusteringBuilder setStrategy(IncrementalClusteringStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    // TODO CHECK
    public IncrementalClusteringBuilder setDataSources(List<String> sources) {
      this.sources = sources;
      return this;
    }

    // TODO CHECK
    public IncrementalClusteringBuilder setEnvironment(ExecutionEnvironment env) {
      this.env = env;
      return this;
    }

    /**
     * Creates link filter based on the configured parameters.
     * @return link filter
     */
    public IncrementalClustering build() {
      if (env != null) {
        if (strategy == IncrementalClusteringStrategy.MINSIZE) {
          return new MinSizeIncClustering(sources, env);
        } else if (strategy == IncrementalClusteringStrategy.FIXED) {
          return new FixedIncrementalClustering(env); // basic test strategy
        } else {
          throw new IllegalArgumentException("Unsupported strategy: " + strategy);
        }
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }
  }
}
