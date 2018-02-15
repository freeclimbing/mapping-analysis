package org.mappinganalysis.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.Properties;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Get a Gelly Vertex representation for north carolina input vertices.
 */
public class GradoopToObjectMapVertexMapper
    implements MapFunction<org.gradoop.common.model.impl.pojo.Vertex, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(GradoopToObjectMapVertexMapper.class);

  private final Vertex<Long, ObjectMap> reuseVertex;

  public GradoopToObjectMapVertexMapper() {
    reuseVertex = new Vertex<>();
  }

  @Override
  public Vertex<Long, ObjectMap> map(org.gradoop.common.model.impl.pojo.Vertex gradoopVertex) throws Exception {
    Properties gradoopProperties = gradoopVertex.getProperties();
    ObjectMap properties = new ObjectMap(Constants.NC);

    assert gradoopProperties != null;
    for (String property : gradoopProperties.getKeys()) {
      if (property.equals("recId")) {
        String idString = gradoopProperties.get(property).getString();

        reuseVertex.setId(Utils.getIdFromNcId(idString));
      } else if (property.equals("name")) {
        properties.setLabel(gradoopProperties.get(property).getString());
      } else if (property.equals("suburb")) {
        properties.setAlbum(gradoopProperties.get(property).getString());
      } else if (property.equals("type")) {
        properties.setDataSource(gradoopProperties.get(property).getString());
      } else if (property.equals("surname")) {
        properties.setArtist(gradoopProperties.get(property).getString());
      } else if (property.equals("postcod")) {
        properties.setNumber(gradoopProperties.get(property).getString());
      } else if (property.equals("clsId")) {
        properties.put("clsId", gradoopProperties.get(property).getLong());
      }
    }

    reuseVertex.setValue(properties);
    return reuseVertex;
  }
}
