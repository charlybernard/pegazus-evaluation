from uuid import uuid4

def create_version_descriptions(factoids):
    source = {
        "uri": "http://example.org/factoids",
        "label": "Factoïdes générés pour les numéros de rue",
        "lang": "fr"
    }
    all_descriptions = {"landmarks": [], "relations": [], "source": source}

    for _, row in factoids.iterrows():
        lm_descriptions, lr_description = create_street_number_state_description(
            sn_label=row["snLabel"],
            th_label=row["thLabel"],
            geom_value=row["versionValue"],
            start_time_stamp=row["startTime"],
            end_time_stamp=row["endTime"],
            lang="fr",
            provenance_uri=row["attrVersion"],
        )
        all_descriptions["landmarks"] += lm_descriptions
        all_descriptions["relations"].append(lr_description)
    
    return all_descriptions

def create_change_descriptions(factoids):
    all_descriptions = []
    for _, row in factoids.iterrows():
        desc = create_streetnumber_attr_geom_change_descriptions(
            sn_label=row["snLabel"],
            th_label=row["thLabel"],
            time_stamp=row["time"],
            lang="fr",
            provenance_uri=row["change"],
        )
        all_descriptions.append(desc)
    
    return {"events": all_descriptions}


def create_streetnumber_attr_geom_change_descriptions(sn_label, th_label, time_stamp, lang, provenance_uri):
    description = {
        "landmarks": [
            {
                "id": 1,
                "label": sn_label,
                "type": "street_number",
                "changes": [
                    {
                    "on": "attribute",
                    "attribute": "geometry"
                    }
                ]
            },
            {
                "id": 2,
                "label": th_label,
                "lang": lang,
                "type": "thoroughfare",
            },
        ],
        "relations": [
            {
                "type": "belongs",
                "id": 1,
                "locatum": 1,
                "relatum": [2],
            },
        ],
        "time": {
        "stamp": time_stamp,
        "calendar": "gregorian",
        "precision": "day"
      },
      "provenance": {
            "uri": provenance_uri,
            "label": f"Factoïde issu d'un changement de géométrie de '{sn_label}, {th_label}'",
            "lang": "fr"
      }
    }
    
    return description
    

def create_street_number_state_description(sn_label, th_label, geom_value, start_time_stamp, end_time_stamp, lang, provenance_uri):
    sn_uuid, th_uuid = str(uuid4()), str(uuid4())

    time_description = {
            "start": {
                "stamp": start_time_stamp,
                "calendar": "gregorian",
                "precision": "day"
            },
            "end": {
                "stamp": end_time_stamp,
                "calendar": "gregorian",
                "precision": "day"
            }
        }

    lm_descriptions = [
        {
            "id": sn_uuid,
            "label": sn_label,
            "type": "street_number",
            "attributes": {
                "geometry": {
                    "value": geom_value,
                    "datatype": "wkt_literal"
                },
                "name": {
                    "value": sn_label
                }
            },
            "time": time_description,
            "provenance": {
                "uri": provenance_uri,
                "label": f"Factoïde issu d'une version de géométrie de '{sn_label}, {th_label}'",
                "lang": "fr"
            }
        },
        {
            "id": th_uuid,
            "label": th_label,
            "lang": "fr",
            "type": "thoroughfare",
            "attributes": {
                "name": {
                    "value": th_label,
                    "lang": lang,
                }
            },
            "time": time_description,
            "provenance": {
                "uri": provenance_uri,
            }
        }
        ]

    lr_description = {
        "type": "belongs",
        "id": 1,
        "locatum": sn_uuid,
        "relatum": [th_uuid],
        "time": time_description,
    }

    return lm_descriptions, lr_description