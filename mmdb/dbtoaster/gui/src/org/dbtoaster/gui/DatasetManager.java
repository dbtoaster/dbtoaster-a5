package org.dbtoaster.gui;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

// Class to hold dataset configuration for demo.
// TODO: represent internals with Eclipse Datatools SQL model.
// TODO: set up gui element to support dataset browsing and definition.
public class DatasetManager {

	public class Dataset {
		// relation name => field name * type
		HashMap<String, LinkedHashMap<String, String>> relations;
		HashMap<String, String> relationSources;
		HashMap<String, String> sourceAdaptors;
		
		public Dataset() {
			relations = new HashMap<String, LinkedHashMap<String,String>>();
			relationSources = new HashMap<String, String>();
			sourceAdaptors = new HashMap<String, String>();
		}
		
		void addRelation(String name, LinkedHashMap<String, String> fieldsAndTypes,
				String source, String adaptor)
		{
			relations.put(name, fieldsAndTypes);
			relationSources.put(name, source);
			sourceAdaptors.put(name, adaptor);
		}
		
		public Set<String> getRelationNames() { return relations.keySet(); }
		
		boolean hasRelation(String name) { return relations.containsKey(name); }

		public LinkedHashMap<String, String> getRelationFields(String name)
		{
			LinkedHashMap<String, String> r = null;
			if ( relations.containsKey(name)) r = relations.get(name);
			return r;
		}
	}
	
	LinkedHashMap<String, Dataset> datasets;
	
	public DatasetManager() {
		datasets = new LinkedHashMap<String, Dataset>();
	}
	
	public void addDataset(String name, Dataset ds)
	{
		if ( datasets.containsKey(name) ) {
			System.out.println("Dataset " + name + " already exists!");
		}
		else {
			datasets.put(name, ds);
		}
	}
	
	public Dataset getDataset(String name)
	{
		Dataset r = null;
		if ( datasets.containsKey(name) ) r = datasets.get(name);
		return r;
	}
	
	public Set<String> getDatasetNames() { return datasets.keySet(); }
	
	boolean hasDataset(String name) { return datasets.containsKey(name); }
	
	boolean hasDatasetRelation(String name, String relName)
	{
		boolean r = false;
		if ( name == null || name.isEmpty() ) {
			for (Map.Entry<String, Dataset> e : datasets.entrySet())
			{
				System.out.println("Checking dataset " + e.getKey());
				r = (r || e.getValue().hasRelation(relName));
			}
		}
		else
			r = hasDataset(name) && getDataset(name).hasRelation(relName);
		
		return r;
	}

	// Find first instance of relation.
	LinkedHashMap<String, String> getRelationFields(String relName)
	{
		LinkedHashMap<String, String> r = null;
		for (Dataset ds : datasets.values()) {
			if ( ds.hasRelation(relName) ) {
				System.out.println("Found fields for " + relName);
				r = ds.getRelationFields(relName);
				return r;
			}
		}
		
		return r;
	}

	public static DatasetManager initDemoDatasetManager()
	{
		DatasetManager aDM = new DatasetManager();
		
		// Orderbook dataset.
		Dataset orderbook = aDM.new Dataset();
		LinkedHashMap<String, String> bookFieldsAndTypes =
			new LinkedHashMap<String, String>();
		
		bookFieldsAndTypes.put("t", "int");
		bookFieldsAndTypes.put("id", "int");
		bookFieldsAndTypes.put("p", "int");
		bookFieldsAndTypes.put("v", "int");
		orderbook.addRelation("bids", bookFieldsAndTypes, "", "CSV");
		orderbook.addRelation("asks", bookFieldsAndTypes, "", "CSV");
		
		aDM.addDataset("orderbook", orderbook);

		// TODO: SSB dataset
		
		// TODO: LinearRoad dataset

		return aDM;
	}
}
