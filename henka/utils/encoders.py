import numpy as np
import math
from datetime import date, datetime
import uuid
from decimal import Decimal
from elasticsearch.serializer import JSONSerializer

class SetEncoder(JSONSerializer):

    def default(self, data):
        if isinstance(data, type(bool)):
            return data
        if isinstance(data, np.bool_):
            return bool(data)
        elif isinstance(data, (date, datetime)):
            return data.isoformat()
        elif isinstance(data, Decimal):
            return float(data)
        elif isinstance(data, uuid.UUID):
            return str(data)
        elif isinstance(data, np.int64):
            return int(data)
        elif isinstance(data, np.float64):
            return float(data)
        elif math.isnan(data):
            return None
        raise TypeError("Unable to serialize %r (type: %s)" % (data, type(data)))
