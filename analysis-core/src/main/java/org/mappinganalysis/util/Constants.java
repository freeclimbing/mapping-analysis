package org.mappinganalysis.util;

/**
 * Constants for mapping analysis
 */
public class Constants {
  /**
   * Database property names.
   */
  public static final String CMD_LL = "linklion";
  public static final String CMD_GEO = "geo";
  public static final String DEFAULT_VALUE = "default";
  public static final String DEBUG = "debug";
  public static final String INFO = "info";
  public static final String LESS = "less";
  public static final String GEO_FULL_NAME = "dbURLfull";
  public static final String DB_PROPERY_FILE_NAME = "db";
  public static final String DB_URL_FIELD = "url";
  /**
   * Accumulators
   */
  public static final String PROP_COUNT_ACCUMULATOR = "prop-count";
  public static final String EDGE_COUNT_ACCUMULATOR = "edge-count";
  public static final String VERTEX_COUNT_ACCUMULATOR = "vertex-count";
  public static final String BASE_VERTEX_COUNT_ACCUMULATOR = "vertex-count";
  public static final String TYPES_COUNT_ACCUMULATOR = "types-count";
  public static final String PREPROC_LINK_FILTER_ACCUMULATOR = "links-filtered-counter";
  public static final String FILTERED_LINKS_ACCUMULATOR = "links-filtered";
  public static final String RESTRICT_EDGE_COUNT_ACCUMULATOR = "restrict-count";
  public static final String SIMSORT_EXCLUDE_FROM_COMPONENT_ACCUMULATOR = "exclude-from-component-counter";
  public static final String EDGE_EXCLUDE_ACCUMULATOR = "edge-exclude-counter";
  public static final String REFINEMENT_MERGE_ACCUMULATOR = "merged-cluster-counter";
  public static final String EXCLUDE_VERTEX_ACCUMULATOR = "exclude-vertex-counter";
  public static final String REPRESENTATIVE_ACCUMULATOR = "representative-counter";
  public static final String VERTEX_OPTIONS = "vertex-options";
  public static final String AGG_PREFIX = "aggregated-";
  /**
   * similarity default values.
   */
  public static final Double MAXIMAL_GEO_DISTANCE = 150000D;
  public static final Double SHADING_TYPE_SIM = 1D;
  public static final String SIM_GEO_LABEL_STRATEGY = "geo-label";
  /**
   * DB attName for gn type detail information
   */
  public static final String GN_TYPE_DETAIL = "typeDetail";
  /**
   * DB column name for 'rdf:type'/..., 'geo:lat/lon', 'rdfs:label'/'gn:name'/... field.
   */
  public static final String TYPE = "type";
  public static final String LAT = "lat";
  public static final String LON = "lon";
  public static final String LABEL = "label";
  /**
   * DB column name for GeoNames second type value field.
   */
  public static final String TYPE_INTERN = "typeIntern";
  public static final String TMP_TYPE = "tmpType" ;
  public static final String COMP_TYPE = "compType";
  /**
   * temp values for missing values
   */
  public static final String NO_LABEL_FOUND = "no_label_found";
  public static final String NO_TYPE = "no_type";
  /**
   * Field name for sim value names
   */
  public static final String SIM_TYPE = "typeMatch";
  public static final String SIM_TRIGRAM = "trigramSim";
  public static final String SIM_DISTANCE = "distance";
  public static final String AGGREGATED_SIM_VALUE = "aggSimValue";
  public static final String VERTEX_AGG_SIM_VALUE = "vertexAggSimValue";
  public static final String VERTEX_STATUS = "vertexStatus";
  public static final Double DEFAULT_VERTEX_SIM = -1D;
  /**
   * field name for connected component ID
   */
  public static final String CC_ID = "ccId";
  public static final String VERTEX_ID = "vertexId";
  public static final String HASH_CC = "hashCc";
  public static final String OLD_HASH_CC = "oldHashCc";
  public static final String REFINE_ID = "refineId";
  /**
   * property name in cluster representative where clustered vertivces are put into.
   */
  public static final String CL_VERTICES = "clusteredVertices";
  /**
   * source name of resource. internal name for db field ontID_fk
   */
  public static final String ONTOLOGY = "ontology";
  public static final String ONTOLOGIES = "ontologies";
  /**
   * source namespaces
   */
  public static final String DBP_NS = "http://dbpedia.org/";
  public static final String GN_NS = "http://sws.geonames.org/";
  public static final String LGD_NS = "http://linkedgeodata.org/";
  public static final String NYT_NS = "http://data.nytimes.com/";
  public static final String FB_NS = "http://rdf.freebase.com/";

  /**
   * Command line option names.
   */
  public static String INPUT_DIR;
  public static String VERBOSITY;
  public static boolean IS_SIMSORT_ENABLED;
  public static Double MIN_CLUSTER_SIM;
  public static Double MIN_LABEL_PRIORITY_SIM;
  public static Double MIN_SIMSORT_SIM;
  public static boolean IGNORE_MISSING_PROPERTIES;
  public static boolean IS_RESTRICT_ACTIVE;
  public static String PRE_CLUSTER_STRATEGY;
  public static boolean IS_LINK_FILTER_ACTIVE;
  public static boolean IS_TGB_DEFAULT_MODE;
}
