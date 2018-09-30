#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#
# An example program that simulates downloading rulings and updating records.
#

from enum import IntEnum
from salty_orm.db.mysql_provider import MySQLDBConnection
from salty_orm.examples.models import RulingModel
from salty_orm.db.query import Q, QOper

dbconn = MySQLDBConnection()
dbconn.db_connect(user='tester', password='****', database='rulings', host='xx.xx.xx.xx', buffered=True)

result = dbconn.db_connected()


class RulingStatus(IntEnum):
    """ Bit values """
    META_DOWNLOADED = 1
    META_CHANGED = 2
    DOC_DOWNLOADED = 10


# loop until there are no documents that need downloading
while True:

    # Only grab 10 records at a time
    rulings = RulingModel(dbconn).objects. \
        filter(Q('status', QOper.O_LT, RulingStatus.DOC_DOWNLOADED.value, placeholder=dbconn.placeholder)). \
        limit(10)

    if not rulings or len(rulings) == 0:
        break

    for ruling in rulings:
        print("downloading: {0}".format(ruling.ruling_no))

        # Download...
        result = True

        if result is True:
            ruling.status = RulingStatus.DOC_DOWNLOADED.value
            ruling.save(cleaned=True)

print("finished.")

dbconn.db_close()
