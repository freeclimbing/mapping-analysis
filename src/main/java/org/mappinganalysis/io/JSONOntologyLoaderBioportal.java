package org.mappinganalysis.io;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;

import org.mappinganalysis.utils.Utils;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public class JSONOntologyLoaderBioportal {

	public void getOntology(String ontoShortName, String apikey) throws IOException {
		
		String current = new java.io.File( "." ).getCanonicalPath().replace("\\", "/").replace(" ", "%20");        
        String dir = current+"/data/ontologies/";
        String fileLabels = ontoShortName+"_uriLabel";
        String fileSyns = ontoShortName+"_uriSyn";
        String fileObs = ontoShortName+"_uriObsStatus";
        Writer fwLabel = new FileWriter(dir+fileLabels+".txt");
        Writer fwSyns = new FileWriter(dir+fileSyns+".txt");
        Writer fwObs = new FileWriter(dir+fileObs+".txt");
        
        boolean alsoSearchObsoletes = true;
        
		String link;
		if(alsoSearchObsoletes){
			link = "http://data.bioontology.org/ontologies/"+ontoShortName+"/classes/" +
					"?apikey="+apikey+"&display_context=false"+"&display_links=false" +
					"&include=prefLabel,synonym,obsolete&format=json&also_search_obsolete=true";	
		}else{
			link = "http://data.bioontology.org/ontologies/"+ontoShortName+"/classes/" +
					"?apikey="+apikey+"&display_context=false"+"&display_links=false" +
					"&include=prefLabel,synonym&format=json";
		}
		System.out.println("############################");
		System.out.println(ontoShortName);
		System.out.println(link);
				
		HttpURLConnection conn = Utils.openUrlConnection(new URL(link));
		BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        JsonObject jsonObject = JsonObject.readFrom(br);

		int pageCount = jsonObject.get("pageCount").asInt();
        System.out.println("Parse "+pageCount+" pages ..");		
		for(int i = 1; i<=pageCount; i++){
			conn.disconnect();
			try{
				conn = Utils.openUrlConnection(new URL(link+"&page="+i));
				br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				jsonObject = JsonObject.readFrom(br);
			}catch(Exception e){
				System.out.println("Current page number = "+i);
				System.out.println(e);
			}
	        JsonArray collection = jsonObject.get("collection").asArray();		
			
			for(JsonValue v : collection.asArray()){
				String label = "";
				if(!v.asObject().get("prefLabel").isNull()){
					label = v.asObject().get("prefLabel").asString();
				}else{
					System.out.println("label is null: "+v.asObject().get("@id"));
				}
				
				JsonArray synonyms = v.asObject().get("synonym").asArray();
				Boolean obsStatus; 
				if(alsoSearchObsoletes){
					obsStatus = v.asObject().get("obsolete").asBoolean();
				}else{
					obsStatus = false;
				}
				
				String uri = v.asObject().get("@id").asString();
				//System.out.println(label + "\t" +synonyms + "\t" + uri);
				
				//write labels, synonyms and obsolete status per URI to csv file
				if(!uri.contains(".well-known/genid")){
					fwLabel.append(uri+"\t"+label+"\n");
					for(JsonValue syn:synonyms) fwSyns.append(uri+"\t"+syn.asString()+"\n");
					fwObs.append(uri+"\t"+obsStatus.toString()+"\n");					
				}
			}
			fwLabel.flush();		
			fwSyns.flush();
			fwObs.flush();	
		}
		System.out.println("Parsed "+pageCount+" pages!");

		fwLabel.close();
		fwSyns.close();
		fwObs.close();
		
		conn.disconnect();
	}
}
