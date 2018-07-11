package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.ObjectMap;

/**
 * Provide a transformation from MusicTriplet to Triplet<Long, ObjectMap, NullValue>.
 */
class MusicTripletToTripletFunction
    implements MapFunction<MergeMusicTriplet, Triplet<Long, ObjectMap, ObjectMap>> {
  private DataDomain dataDomain;
  private String newSource;

  /**
   * Constructor for incremental source addition
   * @param dataDomain data domain
   * @param newSource new source
   */
  public MusicTripletToTripletFunction(DataDomain dataDomain, String newSource) {
    this.dataDomain = dataDomain;
    this.newSource = newSource;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(MergeMusicTriplet triplet) throws Exception {
    Vertex<Long, ObjectMap> source;
    Vertex<Long, ObjectMap> target;

    Vertex<Long, ObjectMap> firstVertex = triplet
        .getSrcTuple()
        .toVertex(dataDomain);
    Vertex<Long, ObjectMap> secondVertex = triplet
        .getTrgTuple()
        .toVertex(dataDomain);

    if (firstVertex.getValue().getDataSourcesList().contains(newSource)) {
      source = firstVertex;
      target = secondVertex;
    } else {
      source = secondVertex;
      target = firstVertex;
    }

    ObjectMap edgeProperties = new ObjectMap(dataDomain);
    edgeProperties.setEdgeSimilarity(triplet.getSimilarity());

    return new Triplet<>(source.getId(),
        target.getId(),
        source.getValue(),
        target.getValue(),
        edgeProperties);
  }
}
