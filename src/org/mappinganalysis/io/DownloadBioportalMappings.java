package org.mappinganalysis.io;

import java.io.IOException;

public class DownloadBioportalMappings {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		String basicLink = "http://bioportal.bioontology.org";
		String apikey = "df344784-0c8c-49c2-8a63-6067039711cd";
		//String sourceOntology = "NCIT";
		//String targetOntology; 
		
		HTMLMappingLoaderBioportal bpm = new HTMLMappingLoaderBioportal();
		
		String[] ontos = {"CHEBI","NCIT","DOID","LOINC","MESH","RXNORM","RADLEX","GALEN","OMIM","FMA","MA","PDQ","NATPRO"};//"NCIT"
		
		for(int i = 0; i<ontos.length;i++){
			for(int j = i+1; j<ontos.length;j++){
				if(!(ontos[i].equals("MA")&&ontos[j].equals("GALEN"))){
					bpm.parseSubPages(basicLink,ontos[i],ontos[j],apikey);
				}
			}
		}
	
		/*
		//Mouse Adult Gross Anatomy Ontology 
		targetOntology = "MA";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//Galen Ontology
		targetOntology = "GALEN";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//Natural Products Ontology
		targetOntology = "NATPRO";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//Online Mendelian Inheritance in Man
		targetOntology = "OMIM";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);	
		
		//Physician Data Query
		targetOntology = "PDQ";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);		
		
		//Radiology Lexicon
		targetOntology = "RADLEX";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//Foundational Model of Anatomy
		targetOntology = "FMA";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
			
		//Chemical Entities of Biological Interest Ontology
		targetOntology = "CHEBI";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
			
		//Logical Observation Identifier Names and Codes
		targetOntology = "LOINC";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//Human Disease Ontology
		targetOntology = "DOID";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//RxNORM
		targetOntology = "RXNORM";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		
		//Medical Subject Headings
		targetOntology = "MESH";
		bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);
		*/
		
		//Mammalian Phenotype Ontology
		//bisher nicht runtergeladen
		//targetOntology = "MPO";
		//bpm.parseSubPages(basicLink,sourceOntology,targetOntology,apikey);

	
		
	}
}
