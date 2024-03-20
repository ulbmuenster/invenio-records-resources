from invenio_records_resources.services.files.transfer import TransferType


class TransferRegistry:
    DEFAULT_TRANSFER_TYPE = 'L'

    def __init__(self):
        self._transfers = {}

    def register(self, transfer_instance, transfer_type=None):
        """Register a new transfer provider."""
        if transfer_type is None:
            transfer_type = transfer_instance.type

        if isinstance(transfer_type, TransferType):
            transfer_type = transfer_type.type

        if transfer_type in self._transfers:
            raise RuntimeError(
                f"Transfer with type '{transfer_type}' " "is already registered."
            )

        self._transfers[transfer_type] = transfer_instance

    def get_transfer(self, transfer_type, **kwargs):
        """Get transfer type."""
        if transfer_type is None:
            transfer_type = self.DEFAULT_TRANSFER_TYPE

        if isinstance(transfer_type, TransferType):
            transfer_type = transfer_type.type

        return self._transfers[transfer_type](**kwargs)
