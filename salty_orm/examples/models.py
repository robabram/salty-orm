#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#
# A sample Salty-ORM model object


from salty_orm.db.query import BaseTableModel


class RulingModel(BaseTableModel):

    id = None  # type: int
    created = None  # type: datetime
    modified = None  # type: datetime
    cross_id = None  # type: int
    ruling_no = None  # type: str
    subject = None  # type: str
    categories = None  # type: str
    ruling_dt = None  # type: datetime
    is_nafta = None  # type: int
    collection = None  # type: str
    related_rulings = None  # type: str
    modified_by = None  # type: str
    modifies = None  # type: str
    revoked_by = None  # type: str
    revokes = None  # type: str
    tariffs = None  # type: str
    status = None  # type: int

    class Meta:
        db_table = 'test_model'


