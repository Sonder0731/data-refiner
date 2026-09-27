from pyspark.storagelevel import StorageLevel


class PERSIST_LEVEL:
    DISK = StorageLevel.DISK_ONLY
    MEMORY = StorageLevel.MEMORY_ONLY
    MEMORY_DISK = StorageLevel.MEMORY_AND_DISK

    _map = {"disk": DISK, "memory": MEMORY, "memory_disk": MEMORY_DISK}

    @classmethod
    def get(cls, key: str):
        return cls._map.get(key.lower())

    @classmethod
    def __class_getitem__(cls, item: str):
        val = cls._map.get(item.lower())
        if val is None:
            raise ValueError(f"Invalid persist level: {item}")
        return val


class FilterLevel:
    # Tag the boolean value with a new column
    TAG: str = "tag"
    # Filter by the condition without new column
    FILTER: str = "filter"
    # Tag the boolean value with a new column and filter
    TAG_AND_FILTER: str = "tag_and_filter"


class DeduplicatorMode:
    # Deduplicate the data by removing duplicates
    DEDUP: str = "dedup"
    # Only keep duplicates
    DUP: str = "dup"
    # Deduplicate the data and keeping duplicates to another place
    DEDUP_WITH_DUP: str = "dedup_with_dup"
