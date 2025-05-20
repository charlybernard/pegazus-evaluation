import functions.graphdb as gd

def select_streetnumbers_labels(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = f"""
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>

    SELECT DISTINCT ?sn ?snLabel ?thLabel
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{
            ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; rdfs:label|skos:altLabel ?snLabel .
            [] a addr:LandmarkRelation ; addr:locatum ?sn ; addr:relatum ?th ; addr:isLandmarkRelationType lrtype:Belongs .
            ?th addr:isLandmarkType ltype:Thoroughfare ; rdfs:label|skos:altLabel ?thLabel .
        }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)


def select_streetnumbers_attr_geom_version_valid_times(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = f"""
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>

    SELECT DISTINCT ?sn ?attrVersion ?startTime ?endTime
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{
            ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
        }}
        ?cgMe addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
        ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.
        ?evME addr:hasTime|addr:hasTimeBefore [addr:timeStamp ?startTime] .
        ?evO addr:hasTime|addr:hasTimeAfter [addr:timeStamp ?endTime] .
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumbers_attr_geom_version_values(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = f"""
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>

    SELECT DISTINCT ?attrVersion ?versionValue
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{
            ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
        }}
        ?attrVersion addr:versionValue ?versionValue .
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumbers_attr_geom_change_valid_times(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    facts_named_graph = gd.get_named_graph_uri_from_name(graphdb_url, repository_name, facts_named_graph_name)
    
    query = f"""
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
    PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>

    SELECT ?sn ?attr ?change ?time ?timeAfter ?timeBefore 
    WHERE {{
        BIND({facts_named_graph.n3()} AS ?gf)
        GRAPH ?gf {{ ?change a addr:AttributeChange ; addr:appliedTo ?attr ; addr:dependsOn ?ev . }}
        ?attr addr:isAttributeType atype:Geometry .
        ?sn a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute ?attr .
        OPTIONAL {{ ?ev addr:hasTime [addr:timeStamp ?time] }} 
        OPTIONAL {{ ?ev addr:hasTimeAfter [addr:timeStamp ?timeAfter] }}
        OPTIONAL {{ ?ev addr:hasTimeBefore [addr:timeStamp ?timeBefore] }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumber_modified_attr_geom_versions(graphdb_url, repository_name, named_graph_names:list, res_query_file):
    named_graph_uris = [gd.get_named_graph_uri_from_name(graphdb_url, repository_name, name) for name in named_graph_names]
    named_graph_filter = ",".join([uri.n3() for uri in named_graph_uris])
    
    query = f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX ofn: <http://www.ontotext.com/sparql/functions/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
    PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>

    SELECT DISTINCT 
    ?attrVersion ?attrVersionSource
    (ofn:asDays(?tStampApp - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppDay)
    (ofn:asDays(?tStampAppBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppBeforeDay)
    (ofn:asDays(?tStampAppAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppAfterDay)
    (ofn:asDays(?tStampDis - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisDay)
    (ofn:asDays(?tStampDisBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisBeforeDay)
    (ofn:asDays(?tStampDisAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisAfterDay)
    WHERE {{
        ?lm a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; addr:hasTrace ?lmTrace .
        GRAPH ?g {{ ?lmTrace a addr:Landmark .}}
        FILTER (?g IN ({named_graph_filter}))
        ?lm addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion] .
        ?cgME addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
        ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.
        ?attrVersion prov:derivedFrom ?attrVersionSource .

        OPTIONAL {{ ?evME addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }}
        OPTIONAL {{ ?evME addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }}
        OPTIONAL {{ ?evME addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }}
        OPTIONAL {{ ?evO addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }}
        OPTIONAL {{ ?evO addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }}
        OPTIONAL {{ ?evO addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)

def select_streetnumber_unmodified_attr_geom_versions(graphdb_url, repository_name, facts_named_graph_name, res_query_file):
    query = f"""
    PREFIX ofn: <http://www.ontotext.com/sparql/functions/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
    PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
    PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>

    SELECT DISTINCT 
    ?attrVersion
    (ofn:asDays(?tStampApp - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppDay)
    (ofn:asDays(?tStampAppBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppBeforeDay)
    (ofn:asDays(?tStampAppAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppAfterDay)
    (ofn:asDays(?tStampDis - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisDay)
    (ofn:asDays(?tStampDisBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisBeforeDay)
    (ofn:asDays(?tStampDisAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisAfterDay)
    WHERE {{
        ?lm a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber .
        ?lm addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion] .
        ?cgME addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
        ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.

        OPTIONAL {{ ?evME addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }}
        OPTIONAL {{ ?evME addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }}
        OPTIONAL {{ ?evME addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }}
        OPTIONAL {{ ?evO addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }}
        OPTIONAL {{ ?evO addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }}
        OPTIONAL {{ ?evO addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }}
    }}
    """

    gd.select_query_to_txt_file(query, graphdb_url, repository_name, res_query_file)