package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.Properties;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.MusicUtils;
import org.mappinganalysis.util.Utils;

/**
 * Get a Gelly Vertex representation for north carolina input vertices.
 */
public class GradoopToObjectMapVertexMapper
    implements MapFunction<org.gradoop.common.model.impl.pojo.Vertex, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(GradoopToObjectMapVertexMapper.class);

  private final Vertex<Long, ObjectMap> reuseVertex;
  private String domain;

  public GradoopToObjectMapVertexMapper(String domain) {
    this.domain = domain;
    this.reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(org.gradoop.common.model.impl.pojo.Vertex gradoopVertex) throws Exception {
    Properties gradoopProperties = gradoopVertex.getProperties();
    ObjectMap properties = new ObjectMap(Constants.NC);

    assert gradoopProperties != null;
    for (String property : gradoopProperties.getKeys()) {
      switch (property) {
        case Constants.REC_ID:
          String idString = gradoopProperties.get(property).getString();

          if (domain.equals(Constants.NC)) {
            reuseVertex.setId(Utils.getIdFromNcId(idString));
          } else if (domain.equals(Constants.MUSIC)) {
            reuseVertex.setId(Long.valueOf(idString));
          }
          break;
        case Constants.NAME:
        case Constants.TITLE:
          properties.setLabel(gradoopProperties.get(property).getString());
          break;
        case Constants.SUBURB:
        case Constants.ALBUM:
          properties.setAlbum(gradoopProperties.get(property).getString());
          break;
        case Constants.TYPE:
          properties.setDataSource(gradoopProperties.get(property).getString());
          break;
        case Constants.SURNAME:
        case Constants.ARTIST:
          properties.setArtist(gradoopProperties.get(property).getString());
          break;
        case Constants.POSTCOD:
        case Constants.NUMBER:
          properties.setNumber(gradoopProperties.get(property).getString());
          break;
        case Constants.CLS_ID:
          properties.put(Constants.CLS_ID, gradoopProperties.get(property).getLong());
          break;
        case Constants.LANGUAGE:
          properties.setLanguage(MusicUtils.fixLanguage(
              gradoopProperties.get(property).getString()));
          break;
        case Constants.YEAR:
          Integer year = MusicUtils.fixYear(gradoopProperties.get(property).getString());
          if (year != null) {
            properties.setYear(year);
          }
          break;
        case Constants.LENGTH:
          Integer length = MusicUtils.fixSongLength(gradoopProperties.get(property).getString());
          if (length != null) {
            properties.setLength(length);
          }
          break;
      }
    }

    reuseVertex.setValue(properties);
    return reuseVertex;
  }
}
