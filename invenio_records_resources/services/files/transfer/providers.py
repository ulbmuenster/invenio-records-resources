from invenio_records_resources.records import FileRecord
from .base import BaseTransfer, TransferStatus
from .types import FETCH_TRANSFER_TYPE, LOCAL_TRANSFER_TYPE
from ...errors import TransferException
from ...uow import TaskOp
from ..tasks import fetch_file


class LocalTransfer(BaseTransfer):
    """Local transfer."""
    type = LOCAL_TRANSFER_TYPE

    def __init__(self, **kwargs):
        """Constructor."""
        super().__init__(**kwargs)

    def init_file(self, record, file_metadata):
        """Initialize a file."""
        uri = file_metadata.pop("uri", None)
        if uri:
            raise Exception("Cannot set URI for local files.")

        file = record.files.create(key=file_metadata.pop("key"), data=file_metadata)

        return file

    def set_file_content(self, record, file, file_key, stream, content_length):
        """Set file content."""
        if file:
            raise TransferException(f'File with key "{file_key}" is committed.')

        super().set_file_content(record, file, file_key, stream, content_length)

    def get_status(self, obj):
        """Get status of a file."""
        # as there is a file object, the file has been uploaded, so return completed
        return TransferStatus.COMPLETED


class FetchTransfer(BaseTransfer):
    """Fetch transfer."""
    type = FETCH_TRANSFER_TYPE

    def __init__(self, **kwargs):
        """Constructor."""
        super().__init__(**kwargs)

    def init_file(self, record, file_metadata):
        """Initialize a file."""
        uri = file_metadata.pop("uri", None)
        if not uri:
            raise Exception("URI is required for fetch files.")

        obj_kwargs = {
            "file": {
                "uri": uri,
                "storage_class": self.type.type,
                "checksum": file_metadata.pop("checksum", None),
                "size": file_metadata.pop("size", None),
            }
        }

        file_key = file_metadata.pop("key")
        file = record.files.create(
            key=file_key,
            data=file_metadata,
            obj=obj_kwargs,
        )

        self.uow.register(
            TaskOp(
                fetch_file,
                service_id=self.service.id,
                record_id=record.pid.pid_value,
                file_key=file_key,
            )
        )
        return file

    def get_status(self, obj: FileRecord) -> TransferStatus:
        """Get status of a file."""
        if obj.file:
            return TransferStatus.COMPLETED
        return TransferStatus.PENDING
