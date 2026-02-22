from abc import ABC, abstractmethod
from typing import Any, Dict, List

class MetadataProvider(ABC):

    @abstractmethod
    def load(self, dataset_ids: List[str]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def load_by_system(self, system_name: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_system_objects(self, system_name: str) -> List[Dict[str, Any]]:
        pass    
    @abstractmethod
    def load_by_object(self, system_name: str,object_name: str) ->  Dict[str, Any]:
        pass