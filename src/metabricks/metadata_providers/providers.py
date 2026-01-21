import os, json, yaml
from typing import List, Dict, Any
from metabricks.core.metadata_provider import MetadataProvider
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class FileMetadataProvider(MetadataProvider):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def _load_file(self, path: str) -> Dict[str, Any]:
        with open(path) as f:
            if path.endswith(".json"):
                return json.load(f)
            return yaml.safe_load(f)

    def load(self, dataset_ids: List[str]) -> List[Dict[str, Any]]:
        """Load multiple manifests from local files by dataset IDs."""
        manifests = []
        for ds_id in dataset_ids:
            for ext in [".json"]:
                path = os.path.join(self.base_path, f"{ds_id}{ext}")
                if os.path.exists(path):
                    manifests.append(self._load_file(path))
                    break
                else:
                    raise FileNotFoundError(f"No manifest found for {ds_id}")
        return manifests

    def load_by_system(self, system_name: str) -> List[Dict[str, Any]]:
        """Load all manifests under a subfolder named after system_name."""
        system_path = os.path.join(self.base_path, system_name)
        if not os.path.isdir(system_path):
            raise FileNotFoundError(f"No folder found for system {system_name}")
        manifests = []
        for file in os.listdir(system_path):
            if file.endswith((".yaml", ".yml", ".json")):
                manifests.append(self._load_file(os.path.join(system_path, file)))
        return manifests
    
    def get_system_objects(self, system_name: str) -> List[Dict[str, Any]]:
        """Load all manifests under a subfolder named after system_name."""
        system_path = os.path.join(self.base_path, system_name)
        if not os.path.isdir(system_path):
            raise FileNotFoundError(f"No folder found for system {system_name}")
        manifests = []
        for file in os.listdir(system_path):
            if file.endswith((".yaml", ".yml", ".json")):
                manifests.append(os.path.splitext(file)[0])
        return manifests
    
    def load_by_object(self, system_name: str,object_name: str) ->  Dict[str, Any]:
        system_path = os.path.join(self.base_path, system_name)
        if not os.path.isdir(system_path):
            print(f"No folder found for system {system_name} and object:{object_name}")
            return None
        file=object_name+".json"
        file_path=os.path.join(system_path, file)
        if os.path.exists(file_path):
            return self._load_file(file_path)
        else:
            return None
            
    
class DeltaMetadataProvider(MetadataProvider):
    def __init__(self, table: str):
        self.spark = SparkSession.getActiveSession()
        self.table = table

    def load(self, dataset_ids: List[str]) -> List[Dict[str, Any]]:
        ds_filter = ",".join([f"'{x}'" for x in dataset_ids])
        rows = (
            self.spark.table(self.table)
            .filter(f"source_dataset_id IN ({ds_filter})")
            .withColumn("rn", F.row_number().over(Window.partitionBy("source_dataset_id").orderBy(F.desc("version"))))
            .filter("rn = 1")
            .collect()
        )
        return [json.loads(r["manifest_json"]) for r in rows]

    def load_by_system(self, system_name: str) -> List[Dict[str, Any]]:
        rows = (
            self.spark.table(self.table)
            .filter(f"source_system = '{system_name}'")
            .withColumn("rn", F.row_number().over(Window.partitionBy("source_dataset_id").orderBy(F.desc("version"))))
            .filter("rn = 1")
            .collect()
        )
        return [json.loads(r["manifest_json"]) for r in rows]
    
    def get_system_objects(self, system_name: str) -> List[Dict[str, Any]]:
        return None