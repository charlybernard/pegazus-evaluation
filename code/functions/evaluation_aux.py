import pandas as pd
import numpy as np
import math
from uuid import uuid4
from dateutil import parser
from datetime import datetime
import random
import functions.geom_processing as gp

################# Generate a random geometry for each street number #################

def get_random_geometry_for_street_number(values, epsg_code, max_distance=5):
    # For each version value, generate a random geometry within 5 meters of the centroid
    crs_to_uri = gp.get_crs_dict().get(epsg_code)
    geom_transformers = gp.get_useful_transformers_for_to_crs(epsg_code, ["EPSG:4326", "EPSG:3857", "EPSG:2154"])
    new_values = {}
    for version, version_value_list in values.items():
        geom = gp.get_point_around_wkt_literal_geoms(version_value_list, crs_to_uri, geom_transformers, max_distance=max_distance)
        wkt_geom = geom.strip()
        new_values[version] = wkt_geom

    return new_values

################# Generate new valid time contained in old valid time for each version value #################

def generate_random_dates_for_versions(versions):
    # Génération des dates aléatoires
    for idx, row in versions.iterrows():
        start_time_col_name = 'startTime'
        end_time_col_name = 'endTime'

        # Get the start and end time from the row
        # Note: Assumes the columns are named 'startTime' and 'endTime'
        # If the columns are named differently, change the following lines accordingly
        start = parser.parse(row[start_time_col_name])
        end = parser.parse(row[end_time_col_name])

        # Convertir en timestamps pour générer aléatoirement
        start_ts = start.timestamp()
        end_ts = end.timestamp()

        t1_ts = get_random_date_between_interval(start_ts, end_ts)
        t2_ts = t1_ts
        loop_nb, max_loop = 0, 10
        while t1_ts == t2_ts or loop_nb <= max_loop:
            t2_ts = get_random_date_between_interval(start_ts, end_ts)
            loop_nb += 1

        # Trier pour garantir t1 < t2
        t1, t2 = sorted([t1_ts, t2_ts])

        start = t1.isoformat() + 'T00:00:00Z'
        end = t2.isoformat() + 'T00:00:00Z'

        if start == end:
            print(f"Warning ! start time and end time have the same value : {start}")

        versions.at[idx, start_time_col_name] = start
        versions.at[idx, end_time_col_name] = end

def get_random_date_between_interval(ts1, ts2):
    """Returns a date without time, randomly between two timestamps"""
    ts = random.uniform(ts1, ts2)
    return datetime.fromtimestamp(ts).date()

################# Generate new changes coherent with generated ones #################

def generate_random_dates_for_changes(changes):
    # Génération des dates aléatoires
    for idx, row in changes.iterrows():
        time_col_name = 'time'
        time_after_col_name = 'timeAfter'
        time_before_col_name = 'timeBefore'

        # Get the start and end time from the row
        # Note: Assumes the columns are named 'startTime' and 'endTime'
        # If the columns are named differently, change the following lines accordingly
        
        try:
            time = parser.parse(row[time_col_name])
        except:
            time = None

        try:
            time_after = parser.parse(row[time_after_col_name])
        except:
            time_after = None

        try:
            time_before = parser.parse(row[time_before_col_name])
        except:
            time_before = None

        if time is not None:
            final_time = time

        elif None not in [time_before, time_after]:
            final_time = random.uniform(time_after, time_before)

        elif time_after is not None:
            final_time = time_after
        
        elif time_before is not None:
            final_time = time_before
        
        else:
            final_time = np.nan

        if final_time is not None:
            final_time = final_time.date().isoformat() + 'T00:00:00Z'

        changes.at[idx, time_col_name] = final_time

##############################################################################

def get_sources_for_versions(df, frag_source_label):
    if frag_source_label is not None:
        unique_sn_labels = df[df["sourceLabel"] == frag_source_label]["label"].unique()
    else:
        unique_sn_labels = df["label"].unique()
    sources_for_versions = {sn: {} for sn in set(unique_sn_labels)}

    for _, row in df.iterrows():
        sn = row["sn"]
        sn_label = row["label"]
        attr_version = row["attrVersion"]
        source_label = row["sourceLabel"]

        if sn_label in unique_sn_labels:
            if attr_version not in sources_for_versions[sn_label]:
                sources_for_versions[sn_label][attr_version] = set()
            sources_for_versions[sn_label][attr_version].add(source_label)

    return sources_for_versions

def get_times_for_changes(df):
    unique_sn_labels = df["label"].unique()
    times_for_changes = {sn: [] for sn in set(unique_sn_labels)}

    for _, row in df.iterrows():
        sn_label = row["label"]
        time = row["timeDay"]
        time_after = row["timeAfterDay"]
        time_before = row["timeBeforeDay"]

        if sn_label in unique_sn_labels:
            times_for_changes[sn_label].append([time, time_after, time_before])

    return times_for_changes

##############################################################################

def get_graph_quality_from_attribute_versions(unmodified_sn, modified_sn, frag_source_label, union=False):
    if union:
        all_sn = set(unmodified_sn) | set(modified_sn)
        nb_versions_eval, sources_eval = {sn:False for sn in all_sn}, {sn:False for sn in all_sn}
    else:
        nb_versions_eval, sources_eval = {}, {}

    for sn, versions in modified_sn.items():
        unmodified_versions = unmodified_sn.get(sn)

        if unmodified_versions is None or len(versions) != len(unmodified_versions):
            same_nb_versions, same_sources = False, False
            print(sn)
            print(unmodified_versions)
            print(versions)
            # print(f"{len(unmodified_versions)} -> {len(versions)} : {len(unmodified_versions) > len(versions)}")
            print("&&&&&&&&")

        else:
            same_nb_versions, same_sources = True, True
            for version, sources in versions.items():
                has_similar_sources = False
                for unmodified_version, unmodified_sources in unmodified_versions.items():
                    subset1, subset2 = sources.copy(), unmodified_sources.copy()
                    subset1.discard(frag_source_label)
                    subset2.discard(frag_source_label)
                    if subset1 == subset2:
                        has_similar_sources = True
                if not has_similar_sources:
                    same_sources = False
                    print(unmodified_versions)
                    print(versions)
                    print(f"{len(unmodified_versions)} -> {len(versions)} : {len(unmodified_versions) > len(versions)}")
                    print("&&&&&&&&")

        sources_eval[sn] = same_sources
        nb_versions_eval[sn] = same_nb_versions

    nb_true_sources = sum(sources_eval.values())
    nb_false_sources = len(sources_eval) - nb_true_sources
    nb_true_nb_versions = sum(nb_versions_eval.values())
    nb_false_nb_versions = len(nb_versions_eval) - nb_true_nb_versions

    sn_with_versions_with_good_sources = {
        "true": nb_true_sources,
        "false": nb_false_sources,
        "total":len(sources_eval),
        "IoU": nb_true_sources/len(sources_eval)
    }

    sn_with_good_nb_of_versions = {
        "true": nb_true_nb_versions,
        "false": nb_false_nb_versions,
        "total":len(nb_versions_eval),
        "IoU": nb_true_nb_versions/len(nb_versions_eval)
    }

    return [sn_with_good_nb_of_versions, sn_with_versions_with_good_sources]

def get_graph_quality_from_attribute_changes(unmodified_sn, modified_sn):

    nb_changes_eval, same_times_eval, coherent_times_eval = {}, {}, {}
    for sn, changes in modified_sn.items():
        unmodified_changes = unmodified_sn.get(sn)

        if len(changes) != len(unmodified_changes):
            same_nb_changes, same_changes, coherent_changes = False, False, False
            # print(sn)
            # print(changes)
            # print(unmodified_changes)
            # print(f"{len(unmodified_changes)} -> {len(changes)} : {len(unmodified_changes) > len(changes)}")
            # print("&&&&&&&&")
        
        else:
            same_nb_changes, same_changes, coherent_changes = True, True, True

            for change in changes:
                has_similar_changes, has_coherent_changes = False, False
                for unmodified_change in unmodified_changes:
                    cg = [None if math.isnan(x) else x for x in change]
                    unmodified_cg = [None if math.isnan(x) else x for x in unmodified_change]
                    if cg[0] is not None and cg[0] == unmodified_cg[0]:
                        has_similar_changes, has_coherent_changes = True, True
                    elif cg[0] is not None and None not in unmodified_cg[1:] and unmodified_cg[1] <= cg[0] and cg[0] >= unmodified_cg[2]:
                        has_coherent_changes = True
                    elif cg[0] is None and [cg[1:] == unmodified_cg[1:]]:
                        has_similar_changes, has_coherent_changes = True, True
                        
                if not has_similar_changes:
                    same_changes = False
                    # print(sn)
                    # print(changes)
                    # print(unmodified_changes)
                    # print(f"{len(unmodified_changes)} -> {len(changes)} : {len(unmodified_changes) > len(changes)}")
                if not has_coherent_changes:
                    coherent_changes = False
                    # print("&&&&&&&&")

        coherent_times_eval[sn] = coherent_changes 
        same_times_eval[sn] = same_changes
        nb_changes_eval[sn] = same_nb_changes

    nb_true_same_changes = sum(same_times_eval.values())
    nb_false_same_changes = len(same_times_eval) - nb_true_same_changes

    nb_true_coherent_changes = sum(coherent_times_eval.values())
    nb_false_coherent_changes = len(coherent_times_eval) - nb_true_coherent_changes

    nb_true_nb_changes = sum(nb_changes_eval.values())
    nb_false_nb_changes = len(nb_changes_eval) - nb_true_nb_changes

    sn_with_changes_with_same_times = {
        "true": nb_true_same_changes,
        "false": nb_false_same_changes,
        "total":len(same_times_eval),
        "IoU": nb_true_same_changes/len(same_times_eval)
    }

    sn_with_changes_with_cohrent_times = {
        "true": nb_true_coherent_changes,
        "false": nb_false_coherent_changes,
        "total":len(coherent_times_eval),
        "IoU": nb_true_coherent_changes/len(coherent_times_eval)
    }

    sn_with_good_nb_of_changes = {
        "true": nb_true_nb_changes,
        "false": nb_false_nb_changes,
        "total":len(nb_changes_eval),
        "IoU": nb_true_nb_changes/len(nb_changes_eval)
    }

    return [sn_with_good_nb_of_changes, sn_with_changes_with_same_times, sn_with_changes_with_cohrent_times]

###############################################################################


def get_ground_truth_version_sources(links_ground_truth_file, sn_without_link_ground_truth_file, source_mapping):
    d1 = get_ground_truth_version_sources_from_links(links_ground_truth_file, source_mapping)
    d2 = get_ground_truth_version_sources_from_unlinked_streetnumbers(sn_without_link_ground_truth_file, source_mapping)

    return d1|d2

def get_ground_truth_version_sources_from_links(links_ground_truth_file, source_mapping):

    order_to_label = {v["order"]: v["label"] for v in source_mapping.values()}

    df = pd.read_csv(links_ground_truth_file)

    unique_sn_labels = df["simp_label"].unique()
    ground_truth_links = {sn: [] for sn in set(unique_sn_labels)}
    ground_truth_grouped_links = {}
    for _, row in df.iterrows():
        label = row["simp_label"]
        table_from = row["table_from"]
        table_to = row["table_to"]
        are_similar_geom = row["are_similar_geom"]
        order_from = source_mapping[table_from]["order"]
        order_to = source_mapping[table_to]["order"]
        ground_truth_links[label].append((order_from, order_to, are_similar_geom))

    for sn in ground_truth_links:
        links = ground_truth_links[sn]
        links.sort()
        
        # Liste pour stocker les groupes
        groups = []

        # Démarrer un groupe avec le premier élément
        current_group = [links[0][0]]

        for source, target, value in links:
            if value:
                # Si le lien est vrai, on continue dans le groupe
                current_group.append(target)
            else:
                # Sinon, on termine le groupe et on en commence un nouveau
                groups.append(current_group)
                current_group = [target]

        # Ne pas oublier d'ajouter le dernier groupe
        groups.append(current_group)
        
        groupes_with_labels = {str(uuid4()): {order_to_label[o] for o in group} for group in groups}

        ground_truth_grouped_links[sn] = groupes_with_labels

    return ground_truth_grouped_links

def get_ground_truth_version_sources_from_unlinked_streetnumbers(sn_without_link_ground_truth_file, source_mapping):
    df = pd.read_csv(sn_without_link_ground_truth_file)

    ground_truth_grouped_links = {}
    for _, row in df.iterrows():
        label = row["simp_label"]
        source = row["table"]
        source_label = source_mapping[source].get("label")
        group = {str(uuid4()):{source_label}}
        ground_truth_grouped_links[label] = group
    
    return ground_truth_grouped_links