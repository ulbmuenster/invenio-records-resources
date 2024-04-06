from invenio_files_rest import current_files_rest
from invenio_files_rest.models import ObjectVersion, ObjectVersionTag, FileInstance

from ...errors import TransferException
from ...uow import TaskOp
from ..tasks import fetch_file
from .base import BaseTransfer, TransferStatus
from .types import (FETCH_TRANSFER_TYPE, LOCAL_TRANSFER_TYPE,
                    REMOTE_TRANSFER_TYPE, MULTIPART_TRANSFER_TYPE)

from invenio_db import db

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

    def set_file_content(self, record, file, file_key, stream, content_length):
        """Set file content."""
        if file:
            raise TransferException(f'File with key "{file_key}" is committed.')

        super().set_file_content(record, file, file_key, stream, content_length)


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


class RemoteTransfer(BaseTransfer):
    """Remote transfer."""

    transfer_type = REMOTE_TRANSFER_TYPE

    @property
    def status(self):
        # always return completed for remote files
        return TransferStatus.COMPLETED


class MultipartTransfer(BaseTransfer):
    transfer_type = MULTIPART_TRANSFER_TYPE

    def init_file(self, record, file_metadata):
        """Initialize a file."""
        uri = file_metadata.pop("uri", None)
        if uri:
            raise Exception("Cannot set URI for local files.")

        parts = file_metadata.pop("parts", None)
        part_size = file_metadata.pop("part_size", None)
        size = file_metadata.get("size", None)
        checksum = file_metadata.get("checksum", None)

        if not parts:
            raise TransferException("Multipart file transfer requires parts.")

        if not size:
            raise TransferException("Multipart file transfer requires file size.")

        file = record.files.create(key=file_metadata.pop("key"), data=file_metadata)

        version = ObjectVersion.create(record.bucket, file.key)
        ObjectVersionTag.create(version, "multipart:parts", str(parts))
        ObjectVersionTag.create(version, "multipart:part_size", str(part_size))

        default_location = version.bucket.location.uri
        default_storage_class = version.bucket.default_storage_class

        file_instance = FileInstance.create()
        version.set_file(file_instance)

        # get the storage backend
        storage = current_files_rest.storage_factory(
            fileinstance=file_instance,
            default_location=default_location,
            default_storage_class=default_storage_class,
        )

        if hasattr(storage, "initialize_multipart_upload"):
            file_instance.set_uri(
                *storage.initialize_multipart_upload(file, version, parts, size, part_size),
                checksum or 'mutlipart:unknown'
            )
        else:
            file_instance.set_uri(
                *self._initialize_local_multipart_upload(storage, file_instance, parts, size, part_size),
                checksum or 'mutlipart:unknown'
            )
        db.session.add(file_instance)
        return file

    def _initialize_local_multipart_upload(self, storage, file_instance, parts, size, part_size):
        storage.initialize(size=size)
        return storage.fileurl, size

    def set_file_content(self, record, file, file_key, stream, content_length):
        """Set file content."""
        raise TransferException("Can not set content for multipart file, "
                                "use the parts instead.")

    def set_file_multipart_content(selfself, record, file, file_key, part, stream, content_length):
        raise NotImplementedError("Not implemented yet.")
