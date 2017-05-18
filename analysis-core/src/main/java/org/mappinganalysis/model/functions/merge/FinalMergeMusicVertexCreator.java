package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

/**
 * Create final Music merge vertices
 */
public class FinalMergeMusicVertexCreator
    extends RichFlatJoinFunction<MergeMusicTuple, Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  @Override
  public void join(MergeMusicTuple tuple,
                   Vertex<Long, ObjectMap> second,
                   Collector<Vertex<Long, ObjectMap>> out) throws Exception {
    if (tuple.isActive()) {
      ObjectMap map = new ObjectMap(Constants.MUSIC);

      if (!tuple.getLabel().equals(Constants.EMPTY_STRING)) {
        map.setLabel(tuple.getLabel());
      }
      if (!tuple.getAlbum().equals(Constants.EMPTY_STRING)) {
        map.setAlbum(tuple.getAlbum());
      }
      if (!tuple.getArtist().equals(Constants.EMPTY_STRING)) {
        map.setArtist(tuple.getArtist());
      }
      if (!tuple.getNumber().equals(Constants.EMPTY_STRING)) {
        map.setNumber(tuple.getNumber());
      }
      if (!tuple.getLang().equals(Constants.EMPTY_STRING)) {
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
              Constants.MUSIC,
              tuple.getIntSources()));
      map.setClusterVertices(
          Sets.newHashSet(tuple.getClusteredElements()));

      out.collect(new Vertex<>(tuple.getId(), map));
    }
  }
}
