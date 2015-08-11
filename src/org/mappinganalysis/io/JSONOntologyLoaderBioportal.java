package org.mappinganalysis.io;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

public class JSONOntologyLoaderBioportal {

	public void getOntology(String ontoShortName, String apikey) throws IOException {
		
	
		String current = new java.io.File( "." ).getCanonicalPath().replace("\\", "/").replace(" ", "%20");        
        String dir = current+"/data/ontologies/";
		
		String link = "http://data.bioontology.org/ontologies/"+ontoShortName+"/classes/?apikey="+apikey;
      
		System.out.println(link);
		
		// Read the file into an Object (The type of this object will be a List, Map, String, Boolean,
		// Number or null depending on the root object in the file).
		Object jsonObject = JsonUtils.fromURL(new URL(link));
		System.out.println(jsonObject.toString());
		
		//@id abgleichen mit meinen URIs
		//prefLabel und synonym auslesen
		
		// Create a context JSON map containing prefixes and definitions
		Map<String, String> context = new HashMap<>();
		// Customise context...
		// Create an instance of JsonLdOptions with the standard JSON-LD options
		JsonLdOptions options = new JsonLdOptions();
		// Customise options...
		// Call whichever JSONLD function you want! (e.g. compact)
		
		Object compact = null;
		try {
			compact = JsonLdProcessor.compact(jsonObject, context, options);

		} catch (JsonLdError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Print out the result (or don't, it's your call!)
		//System.out.println(JsonUtils.toPrettyString(compact));
	}
}
