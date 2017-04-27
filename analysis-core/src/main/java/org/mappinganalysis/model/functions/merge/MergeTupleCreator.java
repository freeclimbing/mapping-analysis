package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.*;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

/**
 * Create basic merge tuples for merge process, properties are transferred from
 * Gelly vertices.
 *
 * Care: Initial MergeTuples have some values set to avoid null pointer exceptions.
 * - therefore, dont use reuse tuples here
 */
public class MergeTupleCreator
    implements MapFunction<Vertex<Long, ObjectMap>, MergeGeoTuple> {
  private static final Logger LOG = Logger.getLogger(MergeTupleCreator.class);
  private DataDomain domain;

  public MergeTupleCreator(DataDomain domain) {
    this.domain = domain;
  }

  @Override
//  @SuppressWarnings("unchecked")
  public MergeGeoTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    if (domain == DataDomain.GEOGRAPHY) {
      MergeGeoTuple tuple = new MergeGeoTuple();

      ObjectMap properties = vertex.getValue();
      properties.setMode(Constants.GEO);

//    LOG.info("PROPERTIES: " + properties.toString() + " " + vertex.getId());
      tuple.setId(vertex.getId());
      tuple.setLabel(properties.getLabel());
      if (properties.hasGeoPropertiesValid()) {
        tuple.setLatitude(properties.getLatitude());
        tuple.setLongitude(properties.getLongitude());
      }
      tuple.setIntTypes(properties.getIntTypes());
      tuple.setIntSources(properties.getIntDataSources());
      tuple.addClusteredElements(properties.getVerticesList());
      tuple.setBlockingLabel(Utils.getBlockingLabel(properties.getLabel()));

//    LOG.info("### CREATE: " + tuple.toString());
      return tuple;
    } else {
    /**
     * MUSIC
     */
//    if (domain == DataDomain.MUSIC) {
//      MergeMusicTuple tuple = new MergeMusicTuple();
//
//      ObjectMap properties = vertex.getValue();
//      tuple.setId(vertex.getId());
//      tuple.setLabel(properties.getLabel());
//
//      tuple.setAlbum(properties.getAlbum());
//      tuple.setArtist(properties.getArtist());
//      tuple.setLength(properties.getLength());
//      tuple.setLang(properties.getLanguage());
//      tuple.setNumber(properties.getNumber());
//      tuple.setYear(properties.getYear());
//
//      tuple.setIntSources(properties.getIntDataSources());
//      tuple.addClusteredElements(properties.getVerticesList());
//      tuple.setBlockingLabel(Utils.getBlockingLabel(properties.getLabel()));
//
////    LOG.info("### CREATE: " + tuple.toString());
//      return (M) tuple;
//    } else {
      throw new IllegalArgumentException("Unsupported domain: " + domain.toString());
    }
  }

//  @Override
//  public TypeInformation<T> getProducedType() {
//    return TypeExtractor.getForClass(T);
//  }

//  @Override
//  public TypeInformation<M> getProducedType() {
//    return TypeExtractor.createTypeInfo();
//  }
}