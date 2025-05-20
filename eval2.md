# Requêtes

## Requêtes pour l'évaluation 2

### 1. Génération de données synthétiques à ajouter

Lancement des requêtes SPARQL (1, 2, 3) dans graphdb et les exporter dans un fichier csv (défini au-dessus de la requête). Les stocker dans le dossier `data`.

#### Requête 0

* Requête pour sélectionner deux valeurs d'une version pour laquelle on a aussi son temps valide
* Grosse requête non utilisée
```
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
SELECT DISTINCT ?sn ?snLabel ?thLabel ?attrVersion ?attrVersionValueTrace ?attrVersionValueTraceBis ?startTime ?endTime
WHERE {
    BIND(<http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts> AS ?gf)
    GRAPH ?gf {
        ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; rdfs:label ?snLabel ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
        [] a addr:LandmarkRelation ; addr:locatum ?sn ; addr:relatum ?th ; addr:isLandmarkRelationType lrtype:Belongs .
        ?th addr:isLandmarkType ltype:Thoroughfare ; rdfs:label ?thLabel .
    }
    ?attrVersion addr:hasTrace ?attrVersionTrace .
    ?attrVersionTrace addr:versionValue ?attrVersionValueTrace.
    OPTIONAL {
        ?attrVersion addr:hasTrace ?attrVersionTraceBis .
        ?attrVersionTraceBis addr:versionValue ?attrVersionValueTraceBis.
        FILTER (?attrVersionTrace != ?attrVersionTraceBis)
    }
    ?cgMe addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
    ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.
    ?evME addr:hasTime|addr:hasTimeAfter [addr:timeStamp ?startTime] .
    ?evO addr:hasTime|addr:hasTimeBefore [addr:timeStamp ?endTime] .
}
```

#### Requête 1
* Sélection des temps valides des versions de type géométrie des StreetNumber du graphe de faits.
* Résultat stocké dans le fichier `streetnumber_attr_geom_version_valid_times_final_graph.csv`
```
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
SELECT DISTINCT ?sn ?attrVersion ?startTime ?endTime
WHERE {
    BIND(<http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts> AS ?gf)
    GRAPH ?gf {
        ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
    }
    ?cgMe addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
    ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.
    ?evME addr:hasTime|addr:hasTimeBefore [addr:timeStamp ?startTime] .
    ?evO addr:hasTime|addr:hasTimeAfter [addr:timeStamp ?endTime] .
}
```

#### Requête 2
* Sélection des valeurs des versions de type géométrie des StreetNumber du graphe de faits.
* Résultat stocké dans le fichier `streetnumber_attr_geom_version_values_final_graph.csv`
```
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
SELECT DISTINCT ?attrVersion ?versionValue
WHERE {
    BIND(<http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts> AS ?gf)
    GRAPH ?gf {
        ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; addr:hasAttribute [addr:isAttributeType atype:Geometry; addr:hasAttributeVersion ?attrVersion].
    }
    ?attrVersion addr:versionValue ?versionValue .
}
```

#### Requête 3
* Pour chaque StreetNumber, afficher l'ensemble des combinaisons possibles de son nom à partir de ces labels et de ceux de sa rue.
* Résultat stocké dans le fichier `streetnumber_labels_final_graph.csv`
```
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
SELECT DISTINCT ?sn ?snLabel ?thLabel
WHERE {
    BIND(<http://localhost:7200/repositories/addresses_from_factoids/rdf-graphs/facts> AS ?gf)
    GRAPH ?gf {
        ?sn a addr:Landmark ;addr:isLandmarkType ltype:StreetNumber ; rdfs:label|skos:altLabel ?snLabel .
        [] a addr:LandmarkRelation ; addr:locatum ?sn ; addr:relatum ?th ; addr:isLandmarkRelationType lrtype:Belongs .
        ?th addr:isLandmarkType ltype:Thoroughfare ; rdfs:label|skos:altLabel ?thLabel .
    }
}
```

### Création des factoïdes
* Une fois les deux fichiers `streetnumber_unmodified_attr_geom_versions.csv` et `streetnumber_modified_attr_geom_versions.csv` générés, il faut lancer le script `evaluation_2_create_factoids.py`. Le fichier de sortie est un `json` (`streetnumber_descriptions_final_graph.json`) qui faudra intégrer dans les données de construction du graphe final avec les données provenant d'autres sources.
    * chaque version de géométrie a un temps valide, une ou plusieurs valeurs et le street number dont il dépend peut avoir plusieurs noms (pour le 6 rue de Charonne, on peut avoir "6 rue de Charonne", "6 R de Charonne", "6 rue charonne"...)
    * pour chaque version sélectionnée, on crée une attestation : un landmark de type StreetNumber dont le temps valide est inclus dans celui de la version, dont la géométrie est proche de la (ou des) valeur(s) associées à la version, et un nom du street number dont la version dépend
    * chaque attestation est décrite dans le fichier de sortie 
* Lancer le script de construction de graphe (`create_graph.ipynb`) :warning: S'assurer du bon chemin du code qu'on lance (qui doit être situé dans le répertoire `pegazus-extension`)

### Extraction des données
* On extrait les données issues de deux graphes :
    * `adresses_from_factoids` : le graphe de base
    * `adresses_from_factoids_evaluation` : le graphe servant d'évaluation, les données de départ sont les même que le précédent, à la différence qu'on y a ajouté les données fragmentaires synthétique
* L'objectif est de voir si les versions de géométrie des street numbers dans les deux graphes sont les mêmes, i.e. pour les mêmes street numbers, il y autant de versions et qui ont le même temps valide.

#### Sélection des versions du graphe d'évaluation
* Requête SPARQL pour récupérer les données dans le graphe où on a ajouté des faux numéros de rue (eval graph):
    * sélectionner les versions de géométrie des numéros de rue
    * les dates de validité associées
    * les labels cachés associés
* Fichier stocké sous le nom `streetnumber_modified_attr_geom_versions.csv`
```
PREFIX ofn: <http://www.ontotext.com/sparql/functions/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
SELECT DISTINCT 
?lm ?label ?attrVersion 
(ofn:asDays(?tStampApp - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppDay)
(ofn:asDays(?tStampAppBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppBeforeDay)
(ofn:asDays(?tStampAppAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppAfterDay)
(ofn:asDays(?tStampDis - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisDay)
(ofn:asDays(?tStampDisBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisBeforeDay)
(ofn:asDays(?tStampDisAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisAfterDay)
WHERE {
    ?lm a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; skos:hiddenLabel ?snLabel .
    ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType lrtype:Belongs ; addr:locatum ?lm ; addr:relatum [skos:hiddenLabel ?thLabel] .
    BIND(CONCAT(?thLabel, "||", ?snLabel) AS ?label)
    ?lm addr:hasTrace ?lmTrace .
    GRAPH <http://localhost:7200/repositories/addresses_from_factoids_evaluation/rdf-graphs/fragmentary_streetnumbers> {
        ?lmTrace a addr:Landmark .
    }
    ?lm addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion] .
    ?cgME addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
    ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.

    OPTIONAL { ?evME addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }
    OPTIONAL { ?evME addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }
    OPTIONAL { ?evME addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }
    OPTIONAL { ?evO addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }
    OPTIONAL { ?evO addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }
    OPTIONAL { ?evO addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }
}
```

#### Sélection des versions du graphe de base
* Même chose que la requête SPARQL ci-dessus, pour le graphe de base (sans faux numéros de rue)
* Fichier stocké sous le nom `streetnumber_unmodified_attr_geom_versions.csv`
```
PREFIX ofn: <http://www.ontotext.com/sparql/functions/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX addr: <http://rdf.geohistoricaldata.org/def/address#>
PREFIX ltype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkType/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX lrtype: <http://rdf.geohistoricaldata.org/id/codes/address/landmarkRelationType/>
PREFIX atype: <http://rdf.geohistoricaldata.org/id/codes/address/attributeType/>
SELECT DISTINCT 
?lm ?label ?attrVersion 
(ofn:asDays(?tStampApp - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppDay)
(ofn:asDays(?tStampAppBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppBeforeDay)
(ofn:asDays(?tStampAppAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampAppAfterDay)
(ofn:asDays(?tStampDis - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisDay)
(ofn:asDays(?tStampDisBefore - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisBeforeDay)
(ofn:asDays(?tStampDisAfter - "0001-01-01"^^xsd:dateTimeStamp) AS ?tStampDisAfterDay)
WHERE {
    ?lm a addr:Landmark ; addr:isLandmarkType ltype:StreetNumber ; skos:hiddenLabel ?snLabel .
    ?lr a addr:LandmarkRelation ; addr:isLandmarkRelationType lrtype:Belongs ; addr:locatum ?lm ; addr:relatum [skos:hiddenLabel ?thLabel] .
    BIND(CONCAT(?thLabel, "||", ?snLabel) AS ?label)
     ?lm addr:hasAttribute [addr:isAttributeType atype:Geometry ; addr:hasAttributeVersion ?attrVersion] .
    ?cgME addr:makesEffective ?attrVersion ; addr:dependsOn ?evME.
    ?cgO addr:outdates ?attrVersion ; addr:dependsOn ?evO.

    OPTIONAL { ?evME addr:hasTime [addr:timeStamp ?tStampApp ; addr:timePrecision ?tPrecApp] }
    OPTIONAL { ?evME addr:hasTimeBefore [addr:timeStamp ?tStampAppBefore ; addr:timePrecision ?tPrecAppBefore] }
    OPTIONAL { ?evME addr:hasTimeAfter [addr:timeStamp ?tStampAppAfter ; addr:timePrecision ?tPrecAppAfter] }
    OPTIONAL { ?evO addr:hasTime [addr:timeStamp ?tStampDis ; addr:timePrecision ?tPrecDis] }
    OPTIONAL { ?evO addr:hasTimeBefore [addr:timeStamp ?tStampDisBefore ; addr:timePrecision ?tPrecDisBefore] }
    OPTIONAL { ?evO addr:hasTimeAfter [addr:timeStamp ?tStampDisAfter ; addr:timePrecision ?tPrecDisAfter] }
}
```

* On lance le script `evaluation2_compare.py` qui fournit en sortie deux fichiers :
    * un fichier contenant toutes les versions du graphe de base décrivant les géométries des numéros de rue pour lesquelles on a ajouté une attestation synthétique
    * un fichier contenant toutes les versions du graphe d'évaluation décrivant les géométries des numéros de rue pour lesquelles on a ajouté une attestation synthétique auxquelles sa version équivalente dans le graphe de base. 
    * une version V du graphe d'évaluation associée à la géométrie du 2 rue de Charonnne dont le temps valide [1887-1889] est associée à son équivalent dans le graphe de base s'il existe (même temps valide, associé à la même entité géographique)
