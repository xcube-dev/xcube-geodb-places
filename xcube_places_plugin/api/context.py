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
from typing import Mapping, Any, Optional, List

import time
import requests
from geopandas import GeoDataFrame
from xcube.server.api import ApiContext
from xcube.server.api import Context
from xcube.constants import LOG

from xcube_geodb.core.geodb import GeoDBClient


class PlacesPluginContext(ApiContext):

    @property
    def config(self) -> Mapping[str, Any]:
        assert self._config is not None
        return self._config

    @config.setter
    def config(self, config: Mapping[str, Any]):
        assert isinstance(config, Mapping)
        self._config = dict(config)

    def __init__(self, root: Context):
        super().__init__(root)
        self.config = dict(root.config)
        self.root = root

    def on_update(self, prev_context: Optional["Context"]):
        if prev_context:
            self.config = prev_context.config
        self._configure_geodb()
        LOG.debug(f'geodb.whoami: {self.geodb.whoami}')
        import threading
        t = threading.Thread(target=self.update_places)
        t.start()

    def update_places(self):
        base_url = f'http://127.0.0.1:{self.root.config["port"]}'  # todo!
        self._wait_for_server_start(base_url)
        LOG.debug('fetching feature data from geoDB...')
        gdfs = self._run_queries()
        LOG.debug('...done.')
        url = f'{base_url}/places'
        LOG.debug(f'Posting feature data to places API at {url}...')
        for gdf in gdfs:
            requests.post(url=url, data=gdf.to_json())
            #  todo - error handling
        LOG.debug(f'...done.')

    @staticmethod
    def _wait_for_server_start(base_url):
        LOG.debug('waiting until server is fully started...')
        while True:
            try:
                requests.get(url=f'{base_url}/places')
                break
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)
        LOG.debug('ready!')

    def _run_queries(self) -> List[GeoDataFrame]:
        gdfs = []
        for place_group in self.config.get('PlaceGroups'):
            query = place_group.get('Query')
            dn = query.split('?')[0]
            db_name = dn.split('_')[0]
            collection_name = '_'.join(dn.split('_')[1:])
            constraints = query.split('?')[1]
            gdfs.append(self.geodb.get_collection(collection_name,
                                                  query=constraints,
                                                  database=db_name))
        return gdfs

    def _configure_geodb(self):
        geodb_conf = self.config.get('XcubePlaces').get('GeoDBConf')
        server_url = geodb_conf['PostgrestUrl']
        server_port = geodb_conf['PostgrestPort']
        client_id = geodb_conf['ClientId']
        client_secret = geodb_conf['ClientSecret']
        auth_domain = geodb_conf['AuthDomain']
        self.geodb = GeoDBClient(server_url=server_url,
                                 server_port=server_port,
                                 client_id=client_id,
                                 client_secret=client_secret,
                                 auth_aud=auth_domain)
