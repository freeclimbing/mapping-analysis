package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

public class JSONToVertexFormatter<VV>
    extends JSONToEntity
    implements MapFunction<String, Vertex<Long, VV>>, ResultTypeQueryable<VV> {

  private static final Logger LOG = Logger.getLogger(JSONToVertexFormatter.class);
  Class vertexClass;

//  public <ABC> JSONToVertexFormatter(Class<ABC> vertexClass) {
//    this.vertexClass = vertexClass;
//  }

  @Override
  public Vertex<Long, VV> map(String value) throws Exception {
    JSONObject jsonVertex = new JSONObject(value);

    LOG.info(vertexClass.toString());
    Long id = jsonVertex.getLong(Constants.ID);
    Object properties = getProperties(jsonVertex, vertexClass);

    return new Vertex<>(id, (VV) properties);

//    if (vertexClass instanceof ObjectMap) {
//      ObjectMap properties = getProperties(jsonVertex);
//
//      return new Vertex<>(id, (VV) properties);
//    } else if (vertexClass instanceof Long) {
//      Long data = jsonVertex.getLong(Constants.DATA);
//
//      return new Vertex<>(id, (VV) data);
//    } else { // FIXME: 9/30/16
//      return new Vertex<>(id, (VV) NullValue.getInstance());
//    }
//
//    Long data = null;
//    ObjectMap properties = null;
//
//    try {
//      data = jsonVertex.getLong(Constants.DATA);
//    } catch (JSONException e) {
//      properties = getProperties(jsonVertex);
//    }
//
//    if (data == null) {
//      return new Vertex<>(id, (VV) properties);
//    } else {
//      return new Vertex<>(id, (VV) data);
//    }
  }

  @Override
  public TypeInformation<VV> getProducedType() {
    return (TypeInformation<VV>) TypeExtractor.createTypeInfo(ObjectMap.class);
  }
}
