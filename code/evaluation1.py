import pandas as pd
from rdflib import URIRef
import functions.data_from_sparql_queries as dfsq
import functions.evaluation_aux as ea

data_folder = "../data/eval_1/"

graphdb_url = URIRef("http://localhost:7200")
repository_name = "addresses_from_factoids"
facts_named_graph_name = "facts"

"""
SPARQL query to get the links from the facts graph

```
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
SELECT DISTINCT
?sn ?attrVersion ?sourceLabel
WHERE {
    BIND(<http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts> AS ?gf)
    GRAPH ?gf {
        ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; rdfs:label ?snLabel ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
        [] a addr:LandmarkRelation ; addr:locatum ?sn ; addr:relatum ?th ; addr:isLandmarkRelationType lrtype:Belongs .
        ?th addr:isLandmarkType ltype:Thoroughfare ; skos:hiddenLabel ?thLabel .
        BIND(CONCAT(?thLabel, "||", ?snLabel) AS ?label)
    } 
    ?attrVersion addr:hasTrace ?traceAttrVers .
    ?traceAttrVers prov:wasDerivedFrom [rico:isOrWasDescribedBy [rdfs:label ?sourceLabel]] .
}
```

Export the results to a CSV file with the following columns:
- label
- sn
- sourceLabel
"""


# Remplace par les chemins vers tes fichiers

facts_graph_file = data_folder + "links_facts_graph.csv"
facts_graph_file = data_folder + "versions_and_sources_from_unmodified_graph.csv"
links_ground_truth_file = data_folder + "links_ground_truth.csv"
sn_without_link_ground_truth_file = data_folder + "sn_without_link_ground_truth.csv"
output_file = data_folder + "eval_1_output.csv"

source_mapping = {
    "cadastre_paris_1807_adresses":{"order":1, "label":"Adresses du cadastre général de Paris de 1807"},
    "atlas_vasserot_1810_adresses":{"order":2, "label":"Cadastre de Paris par îlot : 1810-1836"},
    "atlas_jacoubet_1836_adresses":{"order":3, "label":"Atlas de la ville de Paris de Jacoubet de 1836"},
    "atlas_municipal_1888_adresses":{"order":4, "label":"Adresses du plan de l'atlas municipal de 1888"},
    "ban_adresses":{"order":5, "label":"Base Adresse Nationale"},
    "osm_adresses":{"order":6, "label":"OpenStreetMap"},
}

sn_gt_version_sources = ea.get_ground_truth_version_sources(links_ground_truth_file, sn_without_link_ground_truth_file, source_mapping)
dfsq.select_streetnumbers_attr_geom_version_and_sources(graphdb_url, repository_name, facts_named_graph_name, facts_graph_file)

df_facts_graph = pd.read_csv(facts_graph_file)
unmodified_sn = ea.get_sources_for_versions(df_facts_graph, None)
version_quality_for_states = ea.get_graph_quality_from_attribute_versions(unmodified_sn, sn_gt_version_sources, None)

print("-----------------------------")
print(version_quality_for_states[0])
print(version_quality_for_states[1])
print("-----------------------------")
