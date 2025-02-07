from abc import ABC, abstractmethod


class ReportGenerator(ABC):
    @abstractmethod
    def generate(self, comparison_result, output_path: str):
        """Generate a report file from the comparison results."""
        pass
