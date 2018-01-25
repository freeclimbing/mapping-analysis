package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

/**
 * Create final Music merge vertices
 */
public class FinalMergeMusicVertexCreator
    extends RichFlatJoinFunction<MergeMusicTuple, Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private final String mode;

  FinalMergeMusicVertexCreator(DataDomain domain) {
    if (domain == DataDomain.MUSIC) {
      this.mode = Constants.MUSIC;
    } else if (domain == DataDomain.NC) {
      this.mode = Constants.NC;
    } else {
      this.mode = null;
    }
  }

  @Override
  public void join(MergeMusicTuple tuple,
                   Vertex<Long, ObjectMap> second,
                   Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    if (tuple.isActive()) {
      ObjectMap map = new ObjectMap(mode);

      if (!tuple.getLabel().isEmpty()) {
        map.setLabel(tuple.getLabel());
      }
      if (!tuple.getAlbum().isEmpty()) {
        map.setAlbum(tuple.getAlbum());
      }
      if (!tuple.getArtist().isEmpty()) {
        map.setArtist(tuple.getArtist());
      }
      if (!tuple.getNumber().isEmpty()) {
        map.setNumber(tuple.getNumber());
      }
      if (!tuple.getLang().isEmpty()) {
        map.setLanguage(tuple.getLang());
      }
      if (tuple.getYear() != Constants.EMPTY_INT) {
        map.setYear(tuple.getYear());
      }
      if (tuple.getLength() != Constants.EMPTY_INT) {
        map.setLength(tuple.getLength());
      }

      map.setClusterDataSources(
          AbstractionUtils.getSourcesStringSet(
              mode,
              tuple.getIntSources()));
      map.setClusterVertices(
          Sets.newHashSet(tuple.getClusteredElements()));

      out.collect(new Vertex<>(tuple.getId(), map));
    }
  }
}
