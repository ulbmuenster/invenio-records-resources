from datetime import datetime, timedelta

from invenio_db import db
from invenio_files_rest import current_files_rest
from invenio_files_rest.models import ObjectVersion, FileInstance, ObjectVersionTag

from .... import LinksTemplate
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
        size = file_metadata.pop("size", None)
        checksum = file_metadata.pop("checksum", None)

        if not parts:
            raise TransferException("Multipart file transfer requires parts.")

        if not size:
            raise TransferException("Multipart file transfer requires file size.")

        file = record.files.create(key=file_metadata.pop("key"), data=file_metadata)

        # create the object version and associated file instance that holds the storage_class
        version = ObjectVersion.create(record.bucket, file.key)
        file.object_version = version
        file.object_version_id = version.version_id
        file.commit()

        # create the file instance that will be used to get the storage factory.
        # it might also be used to initialize the file (preallocate its size)
        file_instance = FileInstance.create()
        db.session.add(file_instance)
        version.set_file(file_instance)

        # keep the multipart upload params in tags
        multipart_tags = {
            "parts": parts,
            "part_size": part_size
        }

        # get the storage backend
        storage = current_files_rest.storage_factory(
            fileinstance=file_instance,
            default_location=(version.bucket.location.uri),
            default_storage_class=self.transfer_type,
        )

        # if the storage backend is multipart aware, use it
        if hasattr(storage, "multipart_initialize_upload"):
            storage_uri, multipart_metadata = storage.multipart_initialize_upload(
                parts, size, part_size
            )
            multipart_tags |= multipart_metadata
        else:
            # otherwise use it as a local storage and pre-allocate the file.
            # In this case, the part size is required as we will be uploading
            # the parts in place to not double the space required.
            if not part_size:
                raise TransferException(
                    "Multipart file transfer to local storage requires part_size."
                )
            storage.initialize(size=size)
            storage_uri = storage.fileurl

        self._set_multipart_tags(version, multipart_tags)

        # set the uri on the file instance and potentially the checksum
        file_instance.set_uri(
            storage_uri,
            size,
            checksum or "mutlipart:unknown",
            storage_class=self.transfer_type,
        )

        db.session.add(file_instance)
        return file

    def set_file_content(self, stream, content_length):
        """Set file content."""
        raise TransferException(
            "Can not set content for multipart file, " "use the parts instead."
        )

    def set_file_multipart_content(self, part, stream, content_length):
        """
        Set file content for a part. This method is called for each part of the
        multipart upload when the upload comes through the Invenio server (for example,
        the upload target is a local filesystem and not S3 or another external service).

        :param part: The part number.
        :param stream: The stream with the part content.
        :param content_length: The content length of the part. Must be equal to the
            part_size for all parts except the last one.
        """
        tags = self._get_multipart_tags(self.file_record.object_version)

        storage = current_files_rest.storage_factory(fileinstance=self.file_record.file)
        if storage and hasattr(storage, "multipart_set_content"):
            new_tags = {
                **tags,
                **(storage.multipart_set_content(part, stream, content_length, tags) or {})
            }
            self._set_multipart_tags(self.file_record.object_version, new_tags)

        part_size = int(tags['part_size'])
        parts = int(tags['parts'])

        if part > parts:
            raise TransferException(
                "Part number is higher than total parts sent in multipart initialization."
            )

        if part < parts and content_length != part_size:
            raise TransferException(
                "Size of this part must be equal to part_size sent in multipart initialization."
            )

        storage.update(
            stream,
            seek=(int(part) - 1) * part_size,
            size=content_length,
        )

    def commit_file(self):
        """
        Commit the file. This method is called after all parts have been uploaded.
        It then changes the storage class to local, thus turning the uploaded file
        into a file that can be sent via the configured storage backend.

        This is the same principle that Fetch uses to turn a file from a remote storage
        into a locally served one.
        """
        super().commit_file()

        tags = self._get_multipart_tags(self.file_record.object_version)

        storage = current_files_rest.storage_factory(fileinstance=self.file_record.file)
        if storage and hasattr(storage, "multipart_commit_upload"):
            storage.multipart_commit_upload(tags)

        # change the storage class to local
        file_instance: FileInstance = self.file_record.object_version.file
        file_instance.storage_class = LOCAL_TRANSFER_TYPE
        db.session.add(file_instance)

        # remove multipart upload settings
        self._remote_multipart_tags()

    def delete_file(self):
        """
        If this method is called, we are deleting a file with an active multipart upload.
        """
        storage = current_files_rest.storage_factory(fileinstance=self.file_record.file)
        if storage and hasattr(storage, "multipart_abort_upload"):
            tags = self._get_multipart_tags(self.file_record.object_version)
            return storage.multipart_abort_upload(tags)

    @property
    def status(self):
        # if the storage_class is M, return pending
        # after commit, the storage class is changed to L (same way as FETCH works)
        return TransferStatus.PENDING

    def expand_links(self, identity, self_url):
        # if the storage can expand links, use it
        storage = current_files_rest.storage_factory(fileinstance=self.file_record.file)
        if storage and hasattr(storage, "multipart_links"):
            links = storage.multipart_links(self_url)
            return LinksTemplate(links, {
                'base_url': self_url
            }).expand(identity, self_url)

        # add a local fallback
        tags = self._get_multipart_tags(self.file_record.object_version)
        parts = int(tags.get("parts", 0))
        if not parts:
            raise TransferException(
                "Implementation error: Multipart file missing parts tag."
            )
        return {
            "content": None,  # remove content when multipart upload is not complete
            "parts": [
                {
                    "part": part_no + 1,
                    "url": f"{self_url}/content/{part_no+1}",
                    "expiration": (datetime.utcnow() + timedelta(days=14)).isoformat(),
                }
                for part_no in range(parts)
            ],
        }

    def _get_multipart_tags(self, version):
        tags = ObjectVersionTag.query.filter(
            ObjectVersionTag.key.startswith("multipart:"),
            ObjectVersionTag.version_id == version.version_id,
        ).all()
        return {tag.key.split(":")[1]: tag.value for tag in tags}

    def _set_multipart_tags(self, version, tags):
        existing_tags = ObjectVersionTag.query.filter(
            ObjectVersionTag.key.startswith("multipart:"),
            ObjectVersionTag.version_id == version.version_id,
        ).all()
        existing_tags_by_key = {tag.key: tag for tag in existing_tags}
        for k, v in tags.items():
            v = str(v)
            k = f"multipart:{k}"
            existing_tag = existing_tags_by_key.pop(k, None)
            if existing_tag:
                if existing_tag.value != v:
                    ObjectVersionTag.create_or_update(version, k, v)
            else:
                ObjectVersionTag.create(version, k, v)
        for tag in existing_tags_by_key.keys():
            ObjectVersionTag.delete(version, tag)

    def _remote_multipart_tags(self):
        ObjectVersionTag.query.filter(
            ObjectVersionTag.key.startswith("multipart:"),
            ObjectVersionTag.version_id == self.file_record.object_version_id,
        ).delete(synchronize_session="fetch")
