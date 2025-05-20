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

facts_graph_file = data_folder + "links_from_facts_graph.csv"
ground_truth_file = data_folder + "links_from_ground_truth.csv"
source_mapping_file = data_folder + "sources_mapping.csv"
output_file = data_folder + "eval_1_output.csv"

dfsq.select_streetnumbers_attr_geom_version_and_sources(graphdb_url, repository_name, facts_named_graph_name, facts_graph_file)

# Read the CSV files
df_facts = pd.read_csv(facts_graph_file)
df_ground_truth = pd.read_csv(ground_truth_file)

df_mapping_from = pd.read_csv(source_mapping_file)
df_mapping_from = df_mapping_from.rename(columns=lambda x: x + "_from")
df_mapping_to = pd.read_csv(source_mapping_file)
df_mapping_to = df_mapping_to.rename(columns=lambda x: x + "_to")

df_facts = pd.merge(df_facts, df_mapping_from, left_on="sourceLabel1", right_on="gf_source_from", how="inner", suffixes=('', '_from'))
df_facts = pd.merge(df_facts, df_mapping_to, left_on="sourceLabel2", right_on="gf_source_to", how="inner", suffixes=('', 'to'))

df_facts["join_label"] = df_facts["label"] + "&from=" + df_facts["gt_source_from"] + "&to=" + df_facts["gt_source_to"]
df_ground_truth["join_label"] = df_ground_truth["simp_label"] + "&from=" + df_ground_truth["table_from"] + "&to=" + df_ground_truth["table_to"]
df_jointure = pd.merge(df_facts, df_ground_truth, left_on="join_label", right_on="join_label", how="outer", suffixes=('', '_gt'))
df_jointure.to_csv(output_file, index=False)