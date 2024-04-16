# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-Records-Resources is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""File permissions generators."""
from typing import List, Dict, Union

from invenio_records_permissions.generators import Generator

from invenio_records_resources.services.files.transfer import LOCAL_TRANSFER_TYPE


class IfTransferType(Generator):
    def __init__(self, transfer_type_to_needs: Dict[str, Union[Generator, List[Generator]]],
                 else_: Union[Generator, List[Generator]] = None):
        # convert to dict of lists if not already
        self._transfer_type_to_needs = {
            transfer_type: needs if isinstance(needs, (list, tuple)) else [needs]
            for transfer_type, needs in transfer_type_to_needs.items()
        }

        if not else_:
            else_ = []
        elif not isinstance(else_, (list, tuple)):
            else_ = [else_]

        self._else = else_

    def needs(self, **kwargs):
        """Enabling Needs."""
        record = kwargs["record"]
        file_key = kwargs.get("file_key")
        if not file_key:
            return []       # no needs if file has not been passed
        file_record = record.files.get(file_key)
        if file_record is None:
            return []

        # TODO: multipart initialization should store the transfer type somewhere in the file record,
        # not on the file. This if is a temporary workaround for that (currently other transfer types
        # save local create a record_file.file object during initialization so we can depend on that here)
        transfer_type = file_record.file.storage_class if file_record.file else LOCAL_TRANSFER_TYPE
        if transfer_type not in self._transfer_type_to_needs:
            needs_generators = self._else
        else:
            needs_generators = self._transfer_type_to_needs[transfer_type]

        return [need for x in needs_generators for need in x.needs(**kwargs)]
