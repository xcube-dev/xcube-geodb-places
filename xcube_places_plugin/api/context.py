# The MIT License (MIT)
# Copyright (c) 2021/2022 by the xcube team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import datetime
import json
import os
import re
from typing import Mapping, Any, Optional, List, Dict, Hashable

import dateutil.parser
from geopandas import GeoDataFrame
from xcube.constants import LOG
from xcube.server.api import ApiContext, ApiError
from xcube.server.api import Context
from xcube.webapi.places import PlacesContext
from xcube.webapi.places.context import PlaceGroup
from xcube_geodb.core.geodb import GeoDBClient


class PlacesPluginContext(ApiContext):

    def __init__(self, server_ctx: Context):
        super().__init__(server_ctx)
        self._places_ctx: PlacesContext = server_ctx.get_api_ctx("places")
        self.config = dict(server_ctx.config)
        self.root = server_ctx

    @property
    def config(self) -> Mapping[str, Any]:
        assert self._config is not None
        return self._config

    @config.setter
    def config(self, config: Mapping[str, Any]):
        assert isinstance(config, Mapping)
        self._config = dict(config)

    def on_update(self, prev_context: Optional["Context"]):
        if prev_context:
            self.config = prev_context.config
        if self._configure_geodb():
            LOG.debug(f'geodb.whoami: {self.geodb.whoami}')
            self.update_places()

    def update_places(self):
        LOG.debug('fetching feature data from geoDB...')
        gdfs = self._run_queries()
        LOG.debug('...done.')

        LOG.debug('adding place groups...')
        for gdf in gdfs:
            place_group_config: Dict[Hashable, Any] = dict()
            for k in gdf.attrs.keys():
                place_group_config[k] = gdf.attrs[k]
            place_group = self._create_place_group(place_group_config, gdf)
            dataset_ids = place_group_config.get('DatasetRefs', [])
            self._places_ctx.add_place_group(place_group, dataset_ids)
        LOG.debug('...done.')

    def _create_place_group(self,
                            place_group_config: Dict[Hashable, Any],
                            gdf: GeoDataFrame) -> PlaceGroup:
        place_group_id = place_group_config.get("PlaceGroupRef")
        if place_group_id:
            raise ApiError.InvalidServerConfig(
                "'PlaceGroupRef' cannot be used in a GDF place group"
            )
        place_group_id = self._places_ctx.get_place_group_id_safe(
            place_group_config)

        place_group = self._places_ctx.get_cached_place_group(place_group_id)
        if place_group is None:
            place_group_title = place_group_config.get("Title",
                                                       place_group_id)
            base_url = f'http://{self.root.config["address"]}:' \
                       f'{self.root.config["port"]}'
            property_mapping = self._places_ctx.get_property_mapping(
                base_url, place_group_config)
            source_encoding = place_group_config.get("CharacterEncoding",
                                                     "utf-8")
            place_group = dict(type="FeatureCollection",
                               features=None,
                               id=place_group_id,
                               title=place_group_title,
                               propertyMapping=property_mapping,
                               sourcePaths='None',
                               sourceEncoding=source_encoding)

            self._places_ctx.check_sub_group_configs(place_group_config)
            self._places_ctx.set_cached_place_group(place_group_id,
                                                    place_group)

        self.load_gdf_place_group_features(place_group, gdf)

        return place_group

    @staticmethod
    def load_gdf_place_group_features(
            place_group: PlaceGroup, gdf: GeoDataFrame) -> None:
        features = place_group.get('features')
        if features is not None:
            return features
        feature_collection = json.loads(gdf.to_json())
        for feature in feature_collection['features']:
            PlacesPluginContext._clean_time_name(feature['properties'])
        place_group['features'] = feature_collection['features']

    def _run_queries(self) -> List[GeoDataFrame]:
        gdfs = []
        for place_group in self.config.get('GeoDBConf').get('PlaceGroups'):
            query = place_group.get('Query')
            dn = query.split('?')[0]
            db_name = dn.split('_')[0]
            collection_name = '_'.join(dn.split('_')[1:])
            constraints = query.split('?')[1]
            if 'geometry' not in constraints:
                constraints = re.sub(
                    r'select=(.*?)&', r'select=\1,geometry&', constraints)
            if 'geometry' not in constraints:
                constraints = re.sub(
                    r'select=(.*)$', r'select=\1,geometry', constraints)
            time_aliases = ['time', 'date', 'datetime', 'date-time',
                            'timestamp']
            if not any(time_alias in constraints
                       for time_alias in time_aliases):
                ci = self.geodb.get_collection_info(collection_name,
                                                    database=db_name)
                time_alias = None
                for t in time_aliases:
                    if t in ci['properties']:
                        time_alias = t
                        break
                if time_alias:
                    constraints = constraints.replace('geometry',
                                                      f'geometry,{time_alias}')
            gdf = self.geodb.get_collection(collection_name,
                                            query=constraints,
                                            database=db_name)
            gdf.to_crs(crs='EPSG:4326', inplace=True)
            for k in place_group.keys():
                gdf.attrs[k] = place_group[k]
            gdfs.append(gdf)
        return gdfs

    def _configure_geodb(self) -> bool:
        if 'GeoDBConf' not in self.config:
            return False
        geodb_conf = self.config.get('GeoDBConf')
        server_url = self._get_property_value(geodb_conf,
                                              'GEODB_API_SERVER_URL')
        server_port = self._get_property_value(geodb_conf,
                                               'GEODB_API_SERVER_PORT')
        client_id = self._get_property_value(geodb_conf,
                                             'GEODB_AUTH_CLIENT_ID', True)
        client_secret = self._get_property_value(geodb_conf,
                                                 'GEODB_AUTH_CLIENT_SECRET',
                                                 True)

        # must be mandatory because the default value is wrong, see
        # https://github.com/dcs4cop/xcube-geodb/issues/80
        auth_audience = self._get_property_value(geodb_conf,
                                                 'GEODB_AUTH_AUD',
                                                 True)
        self.geodb = GeoDBClient(server_url=server_url,
                                 server_port=server_port,
                                 client_id=client_id,
                                 client_secret=client_secret,
                                 auth_aud=auth_audience)
        return True

    @staticmethod
    def _get_property_value(geodb_conf: dict, property_name: str,
                            mandatory: bool = False):
        if mandatory:
            if property_name in os.environ:
                return os.environ[property_name]
            elif property_name in geodb_conf:
                return geodb_conf[property_name]
            else:
                raise ValueError(f'mandatory configuration property '
                                 f'{property_name} not set.')
        else:
            if property_name in os.environ:
                return os.environ[property_name]
            elif property_name in geodb_conf:
                return geodb_conf[property_name]
            else:
                return None

    @staticmethod
    def _clean_time_name(properties: Dict):
        illegal_names = ['datetime', 'timestamp', 'date-time', 'date']
        for n in illegal_names:
            if n in properties:
                properties['time'] = dateutil.parser.parse(
                    properties[n]).isoformat()
                del properties[n]
