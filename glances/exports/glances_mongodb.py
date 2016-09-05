# -*- coding: utf-8 -*-
#
# This file is part of Glances.
#
# Copyright (C) 2015 Nicolargo <nicolas@nicolargo.com>
#
# Glances is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Glances is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Mongodb interface class."""

import socket
import sys
from datetime import datetime

from glances.compat import NoOptionError, NoSectionError
from glances.logger import logger
from glances.exports.glances_export import GlancesExport

from pymongo import MongoClient


class Export(GlancesExport):

    """This class manages the mongodb export module."""

    def __init__(self, config=None, args=None):
        """Init the mongodb export IF."""
        super(Export, self).__init__(config=config, args=args)

        # Load the ES configuration file
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.db = None
        self.collection = None
        self.host_name = socket.gethostname()
        self.export_enable = self.load_conf()
        if not self.export_enable:
            sys.exit(2)

        # Init the ES client
        self.client = self.init()

    def load_conf(self, section="mongodb"):
        """Load the mongodb configuration in the Glances configuration file."""
        if self.config is None:
            return False
        try:
            self.host = self.config.get_value(section, 'host')
            self.port = int(self.config.get_value(section, 'port'))
            self.db = self.config.get_value(section, 'db')
            self.username = self.config.get_value(section, 'username')
            self.password = self.config.get_value(section, 'password')
            self.collection = self.config.get_value(section, 'collection')
        except NoSectionError:
            logger.critical("No Mongodb configuration found")
            return False
        except NoOptionError as e:
            logger.critical("Error in the Mongodb configuration (%s)" % e)
            return False
        else:
            logger.debug("Load Mongodb from the Glances configuration file")

        return True

    def init(self):
        """Init the connection to the mongodb server."""
        if not self.export_enable:
            return None

        try:
            client = MongoClient(host=self.host, port=self.port)
            db = client[self.db]
            if not self.username and self.password:
                db.authenticate(self.username, self.password, source='source_database')
            collection = db[self.collection]

        except Exception as e:
            logger.critical("Cannot connect to Mongodb server %s:%s (%s)" % (self.host, self.port, e))
            sys.exit(2)
        else:
            logger.info("Connected to the Mongodb server %s:%s" % (self.host, self.port))

        return collection

    def export(self, name, columns, points):
        """Write the points to the ES server."""
        logger.debug("Export {0} stats to Mongodb".format(name))

        # Create DB input
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html
        actions = []
        for c, p in zip(columns, points):
            action = {
                "_type": name,
                "_hostname": self.host_name,
                "_id": c,
                "timestamp": datetime.now(),
                "_source": {
                    "value": str(p),
                }
            }
            actions.append(action)

        # Write input to the Mongodb
        try:
            self.client.insert_many(actions)
        except Exception as e:
            logger.error("Cannot export {0} stats to Mongodb ({1})".format(name, e))
