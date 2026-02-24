import abc
from abc import abstractmethod
from typing import Any

from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.bundle_storage import BundleStorage


class BundleRegistry(abc.ABC):
    """
    The BundleRegistry class serves as a registry to manage data bundles.

    This class provides functionality to list, load, delete, and register
    data bundles, along with persisting and retrieving their metadata. It acts
    as an interface for various bundle storage systems, enabling management
    of metadata operations. Derived classes must implement the abstract
    methods to handle specific persistence and retrieval of bundle metadata.

    """

    @abstractmethod
    async def list_bundles(self) -> list[dict[str, Any]]:
        """
        Method for listing bundles where each bundle
        is represented as a dictionary containing multiple key-value pairs.

        Returns:
            list[dict[str, Any]]: A list of dictionaries, each representing a bundle with
            specific attributes and details.
        """
        ...

    @abstractmethod
    async def list_bundles_by_name(self, bundle_name: str) -> list[dict[str, Any]]:
        """
        Retrieves a list of bundles filtered by their name.

        Args:
            bundle_name: The name of the bundle used to filter the results.

        Returns:
            A list of dictionaries, where each dictionary contains details of a bundle
            matching the specified name.
        """
        ...

    @abstractmethod
    async def load_bundle_metadata(self, bundle_name: str, bundle_version: str | None) -> dict[str, Any] | None:
        """
        Asynchronously loads metadata for a specified bundle.

        Attempts to retrieve metadata for a bundle identified by its name and optional
        version. Returns None if the metadata cannot be found.

        Args:
            bundle_name: The name of the bundle whose metadata is being loaded.
            bundle_version: The optional version of the bundle. If None, the latest
                version metadata will be retrieved.

        Returns:
            A dictionary containing the metadata of the bundle if available, or None if
            the metadata cannot be retrieved.
        """
        ...

    @abstractmethod
    async def delete_bundle(self):
        """
        Method for deleting a bundle.

        Raises:
            ValueError: If bundle cannot be found
        """
        ...

    @abstractmethod
    async def persist_metadata(self, data_bundle: DataBundle, metadata: dict[str, Any]):
        """
        Persists metadata associated with a data bundle.

        Args:
            data_bundle: The DataBundle object representing the data whose metadata is to
                be persisted.
            metadata: A dictionary containing metadata as key-value pairs. The keys
                represent metadata fields and corresponding values provide the field data.

        Returns:
            None. This method is intended for storing metadata and does not return a value.
        """
        ...

    @abstractmethod
    async def get_bundle_metadata(self, data_bundle: DataBundle, bundle_storage: BundleStorage) -> dict[str, Any]:
        """
        Method for retrieving metadata about a data bundle.

        Args:
            data_bundle (DataBundle): The data bundle object for which metadata needs
                to be fetched.
            bundle_storage (BundleStorage): The storage mechanism handling the bundle,
                used to retrieve associated data or configurations.

        Returns:
            dict[str, Any]: A dictionary containing metadata about the data bundle.
        """
        ...

    async def register_bundle(self, data_bundle: DataBundle, bundle_storage: BundleStorage):
        """
        Registers a data bundle and persists its associated metadata.

        The method processes a given data bundle, retrieves its metadata, and ensures the
        metadata is saved for future reference.

        Args:
            data_bundle (DataBundle): The data bundle to register.
            bundle_storage (BundleStorage): Storage reference related to the data bundle.

        Returns:
            None
        """
        metadata = await self.get_bundle_metadata(data_bundle=data_bundle, bundle_storage=bundle_storage)
        await self.persist_metadata(data_bundle=data_bundle, metadata=metadata)
