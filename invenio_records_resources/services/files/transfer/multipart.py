from invenio_records_resources.services.files.transfer import TransferType
from invenio_records_resources.services.files.transfer.base import BaseTransfer


class MultipartTransfer(BaseTransfer):
    type = TransferType(type="M", is_serializable=False)

    
