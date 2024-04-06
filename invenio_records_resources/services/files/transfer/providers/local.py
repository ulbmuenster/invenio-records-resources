from ....errors import TransferException
from ..base import BaseTransfer
from ..types import LOCAL_TRANSFER_TYPE


class LocalTransfer(BaseTransfer):
    """Local transfer."""

    transfer_type = LOCAL_TRANSFER_TYPE

    def init_file(self, record, file_metadata):
        """Initialize a file."""
        uri = file_metadata.pop("uri", None)
        if uri:
            raise Exception("Cannot set URI for local files.")

        file = record.files.create(key=file_metadata.pop("key"), data=file_metadata)

        return file

    def set_file_content(self, stream, content_length):
        """Set file content."""
        if self.file_record.file is not None:
            raise TransferException(
                f'File with key "{self.file_record.key}" is committed.'
            )

        super().set_file_content(stream, content_length)
