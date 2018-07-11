package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Utils;

/**
 * hungarian inc clustering, merge only if similarity is above threshold
 */
public class DualVertexMergeFlatMapper
    implements FlatMapFunction<Triplet<Long,ObjectMap,ObjectMap>,
    Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(DualVertexMergeFlatMapper.class);

  private DataDomain dataDomain;
  private double minResultSimilarity;

  public DualVertexMergeFlatMapper(DataDomain dataDomain, double minResultSimilarity) {
    this.dataDomain = dataDomain;
    this.minResultSimilarity = minResultSimilarity;
  }

  @Override
  public void flatMap(Triplet<Long, ObjectMap, ObjectMap> triplet,
                      Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    Vertex<Long, ObjectMap> priority = triplet.getSrcVertex();
    Vertex<Long, ObjectMap> minority = triplet.getTrgVertex();

    if (triplet.getEdge().getValue().getEdgeSimilarity() < minResultSimilarity) {
      out.collect(priority);
      out.collect(minority);
    } else {
      ObjectMap priorities;
      ObjectMap minorities = minority.getValue();

      // geo properties
      if (dataDomain == DataDomain.GEOGRAPHY) {
        priorities = Utils.handleGeoProperties(priority, minority);
      } else if (dataDomain == DataDomain.MUSIC) {
        priorities = Utils.handleMusicProperties(priority, minority);
      } else {
        throw new IllegalArgumentException("DualVertexMerge data domain not implemented: "
            + dataDomain);
      }

      // general properties
      priorities.addClusterDataSources(
          minorities.getDataSourcesList());
      priorities.addClusterVertices(
          minorities.getVerticesList());
//      LOG.info("properties after adding id+sources: " + priorities.toString());

      priority.setId(priority.getId() > minority.getId() ? minority.getId() : priority.getId());
      if (priorities.getLabel().length() < minorities.getLabel().length()) {
        priorities.setLabel(minorities.getLabel());
      }

      out.collect(priority);
    }
  }
}
