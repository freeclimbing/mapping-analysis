package org.mappinganalysis.io.impl.json;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.mappinganalysis.util.Constants;

public class JSONToVertexFormatter<VV>
    extends JSONToEntity
    implements MapFunction<String, Vertex<Long, VV>>, ResultTypeQueryable {
  private Class vertexClass;

  public JSONToVertexFormatter(Class<?> vertexClass) {
    this.vertexClass = vertexClass;
  }

  @Override
  public Vertex<Long, VV> map(String value) throws Exception {
    JSONObject jsonVertex = new JSONObject(value);

    Long id = jsonVertex.getLong(Constants.ID);
    Object properties = getProperties(jsonVertex, vertexClass);

    return new Vertex<>(id, (VV) properties);
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<Vertex<Long, VV>> getProducedType() {
    return new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
            TypeExtractor.getForClass(vertexClass));
  }
}
