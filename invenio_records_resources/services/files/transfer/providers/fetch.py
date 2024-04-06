from ...tasks import fetch_file
from ..types import FETCH_TRANSFER_TYPE
from ....uow import TaskOp

from .remote import RemoteTransferBase


class FetchTransfer(RemoteTransferBase):
    """Fetch transfer."""

    transfer_type = FETCH_TRANSFER_TYPE

    def init_file(self, record, file_metadata):

        file = super().init_file(record, file_metadata)

        self.uow.register(
            TaskOp(
                fetch_file,
                service_id=self.service.id,
                record_id=record.pid.pid_value,
                file_key=file.key,
            )
        )
        return file
