# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-Records-Resources is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""File permissions generators."""


from invenio_access.permissions import any_user, system_process
from invenio_records_permissions.generators import Generator
from invenio_search.engine import dsl

from .transfer import LOCAL_TRANSFER_TYPE, FETCH_TRANSFER_TYPE


class LocalFileGeneratorMixin:
    """Mixin to check if a file is local."""
    def is_file_local(self, record, file_key):
        if file_key:
            file_record = record.files.get(file_key)
            # file_record __bool__ returns false for `if file_record`
            file = file_record.file if file_record is not None else None
            return not file or file.storage_class != FETCH_TRANSFER_TYPE
        else:
            file_records = record.files.entries
            for file_record in file_records:
                file = file_record.file
                if file and file.storage_class == FETCH_TRANSFER_TYPE:
                    return False
        return True


class AnyUserIfFileIsLocal(LocalFileGeneratorMixin, Generator):
    """Allows any user."""

    def needs(self, **kwargs):
        """Enabling Needs."""
        if self.is_file_local(kwargs["record"], kwargs.get("file_key")):
            return [any_user]
        else:
            return [system_process]

    def query_filter(self, **kwargs):
        """Match all in search."""
        return dsl.Q("match_all")
