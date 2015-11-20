package org.mappinganalysis.model.functions;

import com.google.common.collect.Maps;
import com.sun.org.apache.bcel.internal.generic.BasicType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.FlinkVertex;
import sun.reflect.generics.tree.BaseType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * helper class, possibly deprecated soon
 */
public class VertexCreator implements MapFunction<FlinkVertex, Vertex<Long, FlinkVertex>> {//}, ResultTypeQueryable {
  private final Vertex<Long, FlinkVertex> reuseVertex;

  public VertexCreator() {
    reuseVertex = new Vertex<>();
  }


  // todo look in jdbcdataloader basicvertexcreator
  @Override
  public Vertex<Long, FlinkVertex> map(FlinkVertex flinkVertex) throws Exception {

//    System.out.println(TypeExtractor.getForObject(flinkVertex));
//    System.out.println(TypeExtractor.getForObject(flinkVertex.getProperties()));

    reuseVertex.setId(flinkVertex.getId());
    reuseVertex.setValue(flinkVertex);

    return reuseVertex;
  }

  // not working like this...
//  @Override
//  public TypeInformation getProducedType() {
//
//    GenericTypeInfo<Map> gtInfo = new GenericTypeInfo<>(Map.class);
//
//    TupleTypeInfo<FlinkVertex> fvInfo = new TupleTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, gtInfo);
//
//
//    return new TupleTypeInfo(Vertex.class, BasicTypeInfo.LONG_TYPE_INFO,
//        TypeExtractor.getForClass(FlinkVertex.class));
//  }
}