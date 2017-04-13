package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.mappinganalysis.graph.LinkFilterFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.impl.LinkFilterStrategy;

/**
 * Link filter builder class.
 *
 * Basic link filter strategy. Search for entities having more than one element from
 * a single data source as edge target and remove all but the best.
 */
public abstract class LinkFilter
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {

  private LinkFilterFunction function;

  public LinkFilter(LinkFilterFunction function) {
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
  public static final class LinkFilterBuilder {
    private LinkFilterStrategy strategy;
    private Boolean removeIsolatedVertices = true;
    private ExecutionEnvironment env = null;

    public LinkFilterBuilder setStrategy(LinkFilterStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public LinkFilterBuilder setRemoveIsolatedVertices(Boolean removeIsolatedVertices) {
      this.removeIsolatedVertices = removeIsolatedVertices;
      return this;
    }

    public LinkFilterBuilder setEnvironment(ExecutionEnvironment env) {
      this.env = env;
      return this;
    }

    /**
     * Creates link filter based on the configured parameters.
     * @return link filter
     */
    public LinkFilter build() {
      if (env != null) {
        if (strategy == LinkFilterStrategy.BASIC) {
          return new BasicLinkFilter(removeIsolatedVertices, env);
        } else if (strategy == LinkFilterStrategy.CLUSTERING) {
          return new ClusteringLinkFilter(env);
        } else {
          throw new IllegalArgumentException("Unsupported strategy: " + strategy);
        }
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }
  }
}
