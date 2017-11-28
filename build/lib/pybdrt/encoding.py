# -*- coding: utf-8 -*-

from __future__ import absolute_import

from json import JSONEncoder, JSONDecoder, loads
from json.decoder import WHITESPACE

#bdrt protobuf
class BdrtJSONEncoder(JSONEncoder):

    def default(self, o):
        return super(BdrtJSONEncoder, self).default(self, o)

class BdrtJSONDecoder(JSONDecoder):

    def decode(self, s, _w=WHITESPACE.match):
        return super(BdrtJSONDecoder, self).decode(s)

def decode(s):
    return loads(s, cls=BdrtJSONDecoder)