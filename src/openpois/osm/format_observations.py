#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module formats OSM changes and versions into observations, which can be more easily
queried and statistically analyzed.
"""

import numpy as np
import pandas as pd

def format_one_observation(
    changes_df: pd.DataFrame,
    versions_df: pd.DataFrame,
    tag_key: str,
    keep_keys: list[str]
) -> pd.DataFrame:
    """
    Format a single POI's changes and versions into observations.

    Args:
        changes_df: DataFrame with changes data for a single POI.
        versions_df: DataFrame with versions data for a single POI.
        tag_key: Key of the tag to format.
        keep_keys: Keys to keep in the observations.

    Returns:
        DataFrame with formatted observations. Each observation has the following columns:
        - id: unique identifier for the POI
        - version: unique identifier for this version of the POI. Observations are
          uniquely identified by `id` + `version`.
        - changeset: unique identifier for this changeset. Changesets can include changes
          to multiple POIs.
        - obs_timestamp: timestamp for this observation (this version of the POI).
        - last_obs_timestamp: timestamp for the previous observation of this POI.
        - last_tag_timestamp: timestamp for the last time the tag was changed prior to this
          observation.
        - last_tag_user: username of the user who last changed the relevant tag
        - user: username of the user who made this observation
        - tag_key: key of the relevant tag
        - tag_value: value of the relevant tag. If the POI was deleted, this will be NA.
        - changed: was the tag changed in this observation? 1 if changed, 0 if unchanged.
        - deleted: was the POI deleted in this observation? 1 if deleted, 0 if not
          deleted. All deleted POIs will have `tag_value` = NA and `changed` = 1.
        - Additionally, there will be columns for OSM grouping tags: these are `amenity`,
          `shop`, `healthcare`, and `leisure` by default. Each grouping tag will list that
          value, if present, at the time of this observation.
    """
    # Setup
    obs_list = []
    names = [
        "version", "changeset", "obs_timestamp", "last_obs_timestamp",
        "last_tag_timestamp", "user", "last_tag_user", "tag_value", "changed", "deleted"
    ] + keep_keys
    # Create a working dictionary for the latest observation, with some starting values
    latest_obs = {name: None for name in names}
    last_tag_timestamp = None
    last_obs_timestamp = None
    last_tag_user = None
    last_tag_value = None
    # Only start recording observaitons when the relevant tag is first added
    add_to_list = False
    # Iterate through all versions of the POI
    version_ids = sorted(versions_df["version"].unique().tolist())
    for v_idx in version_ids:
        version = versions_df.query("version == @v_idx").iloc[0].to_dict()
        changeset = changes_df.query("version == @v_idx").set_index("key")
        latest_obs['version'] = v_idx
        latest_obs['changeset'] = version['changeset']
        latest_obs['obs_timestamp'] = version['timestamp']
        latest_obs['last_obs_timestamp'] = last_obs_timestamp
        latest_obs['last_tag_timestamp'] = last_tag_timestamp
        latest_obs['last_tag_user'] = last_tag_user
        latest_obs['user'] = version['user']
        # Add all of the latest keep keys
        for key in keep_keys:
            if key in changeset.index:
                latest_obs[key] = changeset.loc[key, "value"]
        # Determine what is happening to the tag
        tag_added = (
            (tag_key in changeset.index) and
            (changeset.loc[tag_key, "change"] == "Added")
        )
        tag_changed = (
            (tag_key in changeset.index) and
            (changeset.loc[tag_key, "change"] == "Changed")
        )
        tag_deleted = (
            (tag_key in changeset.index) and
            (changeset.loc[tag_key, "change"] == "Deleted")
        )
        poi_deleted = (
            ('visible' in changeset.index) and
            (changeset.loc['visible', "value"] == "false")
        )
        poi_re_added = (
            add_to_list and
            ('visible' in changeset.index) and
            (changeset.loc['visible', "value"] == "true")
        )
        any_change = (
            tag_added or tag_changed or tag_deleted or poi_deleted or poi_re_added
        )
        latest_obs['changed'] = np.int64(any_change)
        # Only start adding observations to the list after the relevant tag is first added
        if tag_added:
            add_to_list = True
        if tag_added or tag_changed:
            last_tag_value = changeset.loc[tag_key, "value"]
            latest_obs['tag_value'] = last_tag_value
        # When a tag is changed, update the tag timestamp for the *next* observation
        if tag_deleted or poi_deleted:
            latest_obs['tag_value'] = None
        if poi_re_added:
            latest_obs['tag_value'] = last_tag_value
        if any_change:
            # Update timestamps
            last_tag_timestamp = version['timestamp']
            last_tag_user = version['user']
        if add_to_list:
            obs_list.append(pd.DataFrame({k: [v] for k, v in latest_obs.items()}))
            last_obs_timestamp = latest_obs['obs_timestamp']
    # Combine observations from all changesets
    if len(obs_list) > 0:
        formatted_obs_df = pd.concat(obs_list)
        formatted_obs_df['id'] = changes_df.iloc[0, :]["id"]
        formatted_obs_df['tag_key'] = tag_key
        return formatted_obs_df
    else:
        return pd.DataFrame()


def format_observations(
    changes_df: pd.DataFrame,
    versions_df: pd.DataFrame,
    tag_key: str,
    keep_keys: list[str] = ["amenity", "shop", "healthcare", "leisure"]
) -> pd.DataFrame:
    """
    Format changes and versions into observations.

    Args:
        changes_df: DataFrame with changes data.
        versions_df: DataFrame with versions data.
        tag_key: Key of the tag to format.
        keep_keys: Keys to keep in the observations.

    Returns:
        DataFrame with observations.
    """
    observations_df = pd.concat([
        format_one_observation(
            changes_df = changes_df.query("id == @this_id"),
            versions_df = versions_df.query("id == @this_id"),
            tag_key = tag_key,
            keep_keys = keep_keys,
        )
        for this_id in changes_df["id"].unique().tolist()
    ])
    return observations_df
