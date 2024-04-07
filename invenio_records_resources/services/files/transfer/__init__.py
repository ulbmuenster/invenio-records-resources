# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-Records-Resources is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Files transfer."""


from .base import BaseTransfer, TransferStatus
from .providers.multipart import MultipartTransfer
from .providers.fetch import FetchTransfer
from .providers.remote import RemoteTransfer
from .providers.local import LocalTransfer
from .types import (
    FETCH_TRANSFER_TYPE,
    LOCAL_TRANSFER_TYPE,
    REMOTE_TRANSFER_TYPE,
    MULTIPART_TRANSFER_TYPE,
)


__all__ = (
    "BaseTransfer",
    "FETCH_TRANSFER_TYPE",
    "LOCAL_TRANSFER_TYPE",
    "MULTIPART_TRANSFER_TYPE",
    "REMOTE_TRANSFER_TYPE",
    "TransferStatus",
)
