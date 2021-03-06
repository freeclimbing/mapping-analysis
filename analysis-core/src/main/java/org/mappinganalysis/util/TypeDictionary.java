package org.mappinganalysis.util;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Manually created type dictionary to handle rdf:type value over different data sets.
 */
public class TypeDictionary {

  public static final HashMap<String, String> PRIMARY_TYPE;
  static
  {
    PRIMARY_TYPE = Maps.newHashMap();
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Settlement", "Settlement");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Town", "Settlement");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/City", "Settlement");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Village", "Settlement");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/HistoricalSettlement", "Settlement");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/CityDistrict", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.citytown", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.capital_of_administrative_division", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.neighborhood", "Settlement"); // area??
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.place_with_neighborhoods", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.de_city", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.in_city", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.jp_city_town", "Settlement");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.jp_designated_city", "Settlement");
    PRIMARY_TYPE.put("city, village,...", "Settlement");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Village", "Settlement");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/City", "Settlement");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Suburb", "Settlement");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Town", "Settlement");
    PRIMARY_TYPE.put("http://umbel.org/umbel/rc/Village", "Settlement");
    PRIMARY_TYPE.put("http://umbel.org/umbel/rc/Town", "Settlement");
    PRIMARY_TYPE.put("http://umbel.org/umbel/rc/City", "Settlement");
    PRIMARY_TYPE.put("http://schema.org/City", "Settlement");
    PRIMARY_TYPE.put("http://schema.org/Town", "Settlement");

    //water
//    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Glacier", "NaturalPlace");
//    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Lake", "NaturalPlace");
//    PRIMARY_TYPE.put("http://dbpedia.org/ontology/HotSpring", "NaturalPlace");
//    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Glacier", "NaturalPlace");
//    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Bay", "NaturalPlace");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.river", "BodyOfWater");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.lake", "BodyOfWater");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.body_of_water", "BodyOfWater");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/BodyOfWater", "BodyOfWater");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/River", "BodyOfWater");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Lake", "BodyOfWater");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Ocean", "BodyOfWater");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Sea", "BodyOfWater");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Stream", "BodyOfWater");
    PRIMARY_TYPE.put("stream", "BodyOfWater"); //8k
    PRIMARY_TYPE.put("lake", "BodyOfWater"); //5k
    PRIMARY_TYPE.put("glacier(s)", "BodyOfWater"); //2k
    PRIMARY_TYPE.put("bay", "BodyOfWater");
    PRIMARY_TYPE.put("sea", "BodyOfWater");
    PRIMARY_TYPE.put("reservoir(s)", "BodyOfWater"); // todo
    PRIMARY_TYPE.put("cove(s)", "BodyOfWater"); //700
    PRIMARY_TYPE.put("channel", "BodyOfWater");
    PRIMARY_TYPE.put("overfalls", "BodyOfWater");
    PRIMARY_TYPE.put("canal", "BodyOfWater");
    PRIMARY_TYPE.put("fjord", "BodyOfWater");
    PRIMARY_TYPE.put("harbor(s)", "BodyOfWater"); // todo
    PRIMARY_TYPE.put("inlet", "BodyOfWater");
    PRIMARY_TYPE.put("spring(s)", "BodyOfWater");
    PRIMARY_TYPE.put("waterfall(s)", "BodyOfWater");

    //mountain
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Mountain", "Mountain");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/MountainPass", "Mountain");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/MountainRange", "Mountain");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Peak", "Mountain");
    PRIMARY_TYPE.put("mountain", "Mountain"); //80k
    PRIMARY_TYPE.put("hill", "Mountain");//13k
    PRIMARY_TYPE.put("peak", "Mountain");
    PRIMARY_TYPE.put("mountains", "Mountain");
    PRIMARY_TYPE.put("ridge(s)", "Mountain"); //1700
    PRIMARY_TYPE.put("cape", "Mountain");
    PRIMARY_TYPE.put("slope(s)", "Mountain");
    PRIMARY_TYPE.put("valley", "Mountain"); // 700
    PRIMARY_TYPE.put("hills", "Mountain");
    PRIMARY_TYPE.put("pass", "Mountain");
    PRIMARY_TYPE.put("rock", "Mountain");
    PRIMARY_TYPE.put("rocks", "Mountain");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.mountain", "Mountain");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.mountain_pass", "Mountain");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.mountain_range", "Mountain");

    //island
    PRIMARY_TYPE.put("http://umbel.org/umbel/rc/Island", "Island");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Island", "Island");
    PRIMARY_TYPE.put("islands", "Island");
    PRIMARY_TYPE.put("island", "Island");
    PRIMARY_TYPE.put("islet", "Island");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Island", "Island");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.island", "Island");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/geography.island_group", "Island");

    PRIMARY_TYPE.put("http://dbpedia.org/ontology/School", "School");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/University", "School");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/EducationalInstitution", "School");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/School", "School");
    PRIMARY_TYPE.put("school", "School"); //165k
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.academic", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.academic_institution", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.department", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.educational_institution", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.educational_institution_campus", "School");
//    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.field_of_study", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.fraternity_sorority", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.fraternity_sorority_type", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.honorary_degree_recipient", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.school", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.school_category", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.university", "School");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/education.university_system", "School");

    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Country", "Country");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.country", "Country");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/location.uk_overseas_territory", "Country");
    PRIMARY_TYPE.put("dependent political entity", "Country");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Country", "Country");
    PRIMARY_TYPE.put("independent political entity", "Country");
    PRIMARY_TYPE.put("semi-independent political entity", "Country");
    PRIMARY_TYPE.put("http://umbel.org/umbel/rc/Country", "Country");


    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Airport", "ArchitecturalStructure");
    PRIMARY_TYPE.put("airport", "ArchitecturalStructure"); //9k gn
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/AerowayThing", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Airport", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/aviation.airline", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/aviation.airport", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/aviation.airport_operator", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/aviation.waypoint_type", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/aviation.aircraft_manufacturer", "ArchitecturalStructure");

    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Amenity", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Station", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/LaunchPad", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Lock", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Port", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/PowerStation", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/RestArea", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/RouteOfTransportation", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/AmusementParkAttraction", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Arena", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Building", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Gate", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Infrastructure", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/MilitaryStructure", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Mill", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/NoteworthyPartOfBuilding", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Pyramid", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Shrine", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Square", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Tower", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Tunnel", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Venue", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Zoo", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/ShoppingMall", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Stadium", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/RailwayStation", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/Dam", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://dbpedia.org/ontology/SportFacility", "ArchitecturalStructure");

    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.venue", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.building", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.building_complex", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.landscape_project", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.museum", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.structure", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.building_function", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.building_occupant", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.landscape_project", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.skyscraper", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/architecture.type_of_museum", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/transportation.road", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/transportation.bridge", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/zoos.zoo", "ArchitecturalStructure");
    PRIMARY_TYPE.put("http://rdf.freebase.com/ns/zoos.zoo_exhibit", "ArchitecturalStructure");

    // spot building farm
    PRIMARY_TYPE.put("railroad station", "ArchitecturalStructure"); //8k
    PRIMARY_TYPE.put("tower", "ArchitecturalStructure"); //2600
    PRIMARY_TYPE.put("building(s)", "ArchitecturalStructure"); //2100
    PRIMARY_TYPE.put("stadium", "ArchitecturalStructure");
    PRIMARY_TYPE.put("church", "ArchitecturalStructure");
    PRIMARY_TYPE.put("museum", "ArchitecturalStructure");
    PRIMARY_TYPE.put("hostpital", "ArchitecturalStructure"); //1000
    PRIMARY_TYPE.put("castle", "ArchitecturalStructure");
    PRIMARY_TYPE.put("airfield", "ArchitecturalStructure");
    // general gn type
    PRIMARY_TYPE.put("spot, building, farm, ...", "ArchitecturalStructure");

    PRIMARY_TYPE.put("http://linkedgeodata.org/ontology/Leisure", "ArchitecturalStructure");
    // http://rdf.freebase.com/ns/location.statistical_region, http://rdf.freebase.com/ns/location.neighborhood]
  }

  public static final HashMap<String, String> SECONDARY_TYPE;
  static
  {
    SECONDARY_TYPE = Maps.newHashMap();

    SECONDARY_TYPE.put("http://dbpedia.org/ontology/ArchitecturalStructure", "ArchitecturalStructure");

//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/NaturalPlace", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Archipelago", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Beach", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Cave", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Crater", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Desert", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Valley", "NaturalPlace");
//    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Volcano", "NaturalPlace");
//    SECONDARY_TYPE.put("volcano", "NaturalPlace");//165

//    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/geography.geographical_feature", "NaturalPlace");
    // general gn type
//    SECONDARY_TYPE.put("mountain, hill, rock, ...", "NaturalPlace");
//    SECONDARY_TYPE.put("stream, lake, ...", "NaturalPlace");

    SECONDARY_TYPE.put("http://dbpedia.org/ontology/AdministrativeRegion", "AdministrativeRegion");
    SECONDARY_TYPE.put("country, state, region ...", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.administrative_division", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.statistical_region", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.us_state", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.fr_region", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.province", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.in_state", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.mx_state", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.es_autonomous_community", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.fr_department", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/location.it_comune", "AdministrativeRegion");
    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Region", "AdministrativeRegion");

    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/amusement_parks.park", "ArchitecturalStructure");

    SECONDARY_TYPE.put("http://linkedgeodata.org/ontology/Park", "Park");
    SECONDARY_TYPE.put("http://dbpedia.org/ontology/Park", "Park");
    SECONDARY_TYPE.put("parks,area, ...", "Park"); // TODO area is not always similar to park
    SECONDARY_TYPE.put("http://dbpedia.org/ontology/ProtectedArea", "Park"); // TODO protected area is not always park
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/protected_sites.governing_body_of_protected_sites", "Park");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/protected_sites.listed_site", "Park");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/protected_sites.natural_or_cultural_preservation_agency", "Park");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/protected_sites.park_system", "Park");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/protected_sites.protected_site", "Park");
    SECONDARY_TYPE.put("http://rdf.freebase.com/ns/protected_sites.site_listing_category", "Park");
  }

  public static final HashSet<String> SHADED_TYPES;
  static {
    SHADED_TYPES = Sets.newHashSet();
    SHADED_TYPES.add("ArchitecturalStructure");
    SHADED_TYPES.add("Mountain");
    SHADED_TYPES.add("AdministrativeRegion");
    SHADED_TYPES.add("BodyOfWater");
    SHADED_TYPES.add("Park");
  }

  public static final HashMap<String, String> TYPE_SHADINGS;
  static {
    TYPE_SHADINGS = Maps.newHashMap();
    TYPE_SHADINGS.put("ArchitecturalStructure", "School");
    TYPE_SHADINGS.put("Mountain", "Island");
    TYPE_SHADINGS.put("Country", "Settlement");
    TYPE_SHADINGS.put("Settlement", "AdministrativeRegion");
    TYPE_SHADINGS.put("AdministrativeRegion", "Country");
  }


  /**
   * not enough values to use, to broad context, etc.
   */
  @Deprecated
  public static final HashMap<String, String> TERTIARY_TYPE;
  static {
    TERTIARY_TYPE = Maps.newHashMap();
    TERTIARY_TYPE.put("http://dbpedia.org/ontology/HistoricPlace", "HistoricPlace");
    TERTIARY_TYPE.put("http://dbpedia.org/ontology/Place", "Place");
    TERTIARY_TYPE.put("http://dbpedia.org/ontology/PopulatedPlace", "PopulatedPlace");
    TERTIARY_TYPE.put("http://dbpedia.org/ontology/Location", "Location");
    TERTIARY_TYPE.put("http://rdf.freebase.com/ns/location.location", "Location");
    TERTIARY_TYPE.put("http://rdf.freebase.com/ns/location.dated_location", "Location");
    TERTIARY_TYPE.put("http://umbel.org/umbel/rc/Location_Underspecified", "Location");
    TERTIARY_TYPE.put("http://linkedgeodata.org/ontology/Place", "Place");
    TERTIARY_TYPE.put("http://dbpedia.org/ontology/NaturalPlace", "NaturalPlace");
    TERTIARY_TYPE.put("http://linkedgeodata.org/ontology/NaturalThing", "NaturalPlace");

    TERTIARY_TYPE.put("http://schema.org/Place", "Place");
    TERTIARY_TYPE.put("http://umbel.org/umbel/rc/PopulatedPlace", "PopulatedPlace");
  }
}
