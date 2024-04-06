from ..base import BaseTransfer, TransferStatus
from ..types import REMOTE_TRANSFER_TYPE


class RemoteTransferBase(BaseTransfer):

    def init_file(self, record, file_metadata):
        """Initialize a file."""
        uri = file_metadata.pop("uri", None)
        if not uri:
            raise Exception("URI is required for fetch files.")

        obj_kwargs = {
            "file": {
                "uri": uri,
                "storage_class": self.transfer_type,
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

        return file

    @property
    def transfer_data(self):
        """Transfer file."""

        return super().transfer_data | {
            "uri": self.file_record.file.uri,
        }


class RemoteTransfer(BaseTransfer):
    """Remote transfer."""

    transfer_type = REMOTE_TRANSFER_TYPE

    @property
    def status(self):
        # always return completed for remote files
        return TransferStatus.COMPLETED
