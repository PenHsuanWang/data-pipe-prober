from abc import ABC, abstractmethod


class DataComparer(ABC):
    @abstractmethod
    def compare(self, df1, df2):
        """Compare two DataFrames and return the differences."""
        raise NotImplementedError
