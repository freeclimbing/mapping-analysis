package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.model.impl.LinkFilterStrategy;
import org.mappinganalysis.util.Constants;

import java.util.List;

/**
 * Default (geographic) preprocessing: remove duplicate links, add cc ids,
 * type mismatch correction, link similarity
 */
public class DefaultPreprocessing
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(DefaultPreprocessing.class);

  private final ExecutionEnvironment env;
  private final boolean linkFilterEnabled;
  private final DataDomain domain;

  /**
   * Basic preprocessing: link filter enabled
   * At the end of preprocessing, neither ccId nor hashCcId is correct!! Do not group on these keys.
   */
  public DefaultPreprocessing(ExecutionEnvironment env) {
    this(true, env);
  }

  /**
   * Preprocessing with optional link filter.
   */
  public DefaultPreprocessing(boolean isBasicLinkFilterEnabled, ExecutionEnvironment env) {
    this.linkFilterEnabled = isBasicLinkFilterEnabled;
    this.env = env;
    this.domain = DataDomain.GEOGRAPHY; // CHECK THIS TODO
  }

  /**
   * Music constructor, link filter enabled by default.
   */
  public DefaultPreprocessing(DataDomain domain, ExecutionEnvironment env) {
    this.domain = domain;
    this.linkFilterEnabled = true;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    Graph<Long, ObjectMap, NullValue> tmpGraph = graph
        .mapVertices(new InternalTypeMapFunction())
        .mapVertices(new DataSourceMapFunction())
        .run(new EqualDataSourceLinkRemover(env));

    Graph<Long, ObjectMap, ObjectMap> resultGraph = null;
    List<String> sources = null;
    if (domain == DataDomain.MUSIC) {
      resultGraph = tmpGraph
          .run(new BasicEdgeSimilarityComputation(Constants.MUSIC, env));
      sources = Constants.MUSIC_SOURCES;
    } else if (domain == DataDomain.GEOGRAPHY) {
      resultGraph = tmpGraph
          .run(new TypeMisMatchCorrection(env))
          .run(new BasicEdgeSimilarityComputation(Constants.GEO, env));
      sources = Constants.GEO_SOURCES;
    } else if (domain == DataDomain.NC) {
      resultGraph = tmpGraph
          .run(new BasicEdgeSimilarityComputation(Constants.NC, env));
      sources = Constants.NC_SOURCES;
    }

    if (linkFilterEnabled) {
      LinkFilter linkFilter;
      if (domain == DataDomain.NC) {
        linkFilter = new LinkFilter
            .LinkFilterBuilder()
            .setEnvironment(env)
            .setRemoveIsolatedVertices(false)
            .setDataSources(sources)
            .setStrategy(LinkFilterStrategy.BASIC)
            .build();
      } else {
        linkFilter = new LinkFilter
            .LinkFilterBuilder()
            .setEnvironment(env)
            .setRemoveIsolatedVertices(true)
            .setDataSources(sources)
            .setStrategy(LinkFilterStrategy.BASIC)
            .build();
      }

      assert resultGraph != null;
      return resultGraph
          .run(linkFilter)
          .run(new TypeOverlapCcCreator(domain, env)); // each vertex has "no type" with music/nc dataset
    } else {
      return resultGraph;
    }
  }

  /**
   * Temporary map function for compatibility with old 'ontology' values.
   */
  private static class DataSourceMapFunction implements MapFunction<Vertex<Long,ObjectMap>, ObjectMap> {
    @Override
    public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
      vertex.getValue().getDataSource();

      if (vertex.getId() == 704781154L)
        LOG.info("dataSourceMap: " + vertex.getValue().toString());
      return vertex.getValue();
    }
  }
}
