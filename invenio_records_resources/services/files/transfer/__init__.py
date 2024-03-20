# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-Records-Resources is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Files transfer."""


from .providers import LocalTransfer, FetchTransfer
from .base import Transfer
from .types import TransferType, LOCAL_TRANSFER_TYPE, FETCH_TRANSFER_TYPE, REMOTE_TRANSFER_TYPE


__all__ = ("Transfer", "TransferType", "LocalTransfer", "FetchTransfer",
           "LOCAL_TRANSFER_TYPE", "FETCH_TRANSFER_TYPE", "REMOTE_TRANSFER_TYPE")
