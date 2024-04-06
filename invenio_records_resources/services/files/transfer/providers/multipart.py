from datetime import datetime, timedelta

from invenio_db import db
from invenio_files_rest import current_files_rest
from invenio_files_rest.models import ObjectVersion, FileInstance, ObjectVersionTag

from ....errors import TransferException
from ..base import BaseTransfer, TransferStatus
from ..types import MULTIPART_TRANSFER_TYPE, LOCAL_TRANSFER_TYPE


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
        file.object_version = version
        file.object_version_id = version.version_id
        file.commit()

        default_location = version.bucket.location.uri

        file_instance = FileInstance.create()
        db.session.add(file_instance)
        version.set_file(file_instance)

        # get the storage backend
        storage = current_files_rest.storage_factory(
            fileinstance=file_instance,
            default_location=default_location,
            default_storage_class=self.transfer_type,
        )

        ObjectVersionTag.create(version, "multipart:parts", str(parts))
        ObjectVersionTag.create(version, "multipart:part_size", str(part_size))

        if hasattr(storage, "initialize_multipart_upload"):
            file_instance.set_uri(
                *storage.initialize_multipart_upload(
                    file, version, parts, size, part_size
                ),
                checksum or "mutlipart:unknown",
                storage_class=self.transfer_type,
            )
        else:
            file_instance.set_uri(
                *self._initialize_local_multipart_upload(
                    storage, file_instance, parts, size, part_size
                ),
                checksum or "mutlipart:unknown",
                storage_class=self.transfer_type,
            )
        db.session.add(file_instance)
        return file

    def _initialize_local_multipart_upload(
        self, storage, file_instance, parts, size, part_size
    ):
        if not part_size:
            raise TransferException(
                "Multipart file transfer to local storage requires part_size."
            )
        storage.initialize(size=size)
        return storage.fileurl, size

    @property
    def status(self):
        # if the storage_class is M, return pending
        # after commit, the storage class is changed to L (same way as FETCH works)
        return TransferStatus.PENDING

    def expand_links(self, identity, self_url):
        # if the storage can expand links, use it
        storage = current_files_rest.storage_factory(fileinstance=self.file_record.file)
        if storage and hasattr(storage, "multipart_expand_links"):
            return storage.multipart_expand_links(identity, self.file_record)

        # add a local fallback
        parts = ObjectVersionTag.query.filter(
            ObjectVersionTag.key == "multipart:parts",
            ObjectVersionTag.version_id == self.file_record.object_version_id,
        ).one_or_none()
        if not parts:
            raise TransferException(
                "Implementation error: Multipart file missing parts tag."
            )
        return {
            "content": None,  # remove content when multipart upload is not complete
            "parts": [
                {
                    "part": 1,
                    "url": f"{self_url}/content/{part_no}",
                    "expiration": (datetime.utcnow() + timedelta(days=14)).isoformat(),
                }
                for part_no in range(int(parts.value))
            ],
        }

    def set_file_content(self, stream, content_length):
        """Set file content."""
        raise TransferException(
            "Can not set content for multipart file, " "use the parts instead."
        )

    def set_file_multipart_content(self, part, stream, content_length):
        storage = current_files_rest.storage_factory(fileinstance=self.file_record.file)
        if storage and hasattr(storage, "multipart_set_content"):
            return storage.multipart_set_content(part, stream, content_length)
        part_size = int(
            ObjectVersionTag.query.filter(
                ObjectVersionTag.key == "multipart:part_size",
                ObjectVersionTag.version_id == self.file_record.object_version_id,
            )
            .one()
            .value
        )
        storage.update(
            stream,
            seek=(int(part) - 1) * part_size,
            size=content_length,
        )

    def commit_file(self):
        super().commit_file()
        # change the storage class to local
        file_instance: FileInstance = self.file_record.object_version.file
        file_instance.storage_class = LOCAL_TRANSFER_TYPE
        db.session.add(file_instance)
