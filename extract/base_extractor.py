import os
from pymongo import MongoClient
from abc import ABC, abstractmethod
import logging

class BaseExtractor(ABC):
    def __init__(self, mongodb_uri, db_name, collection_name, checkpoint_collection="etl_checkpoints", client=None):
        self.mongodb_uri = mongodb_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.checkpoint_collection_name = checkpoint_collection
        
        if client:
            self.client = client
        else:
            self.client = MongoClient(self.mongodb_uri)
            
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.checkpoints = self.db[self.checkpoint_collection_name]
        
        self.logger = logging.getLogger(f"airflow.task.{self.__class__.__name__}")

    def get_checkpoint(self, source_id):
        """Retrieves the last checkpoint for a given source."""
        checkpoint = self.checkpoints.find_one({"_id": source_id})
        return checkpoint.get("last_value") if checkpoint else None

    def save_checkpoint(self, source_id, value):
        """Saves the checkpoint for a given source."""
        self.checkpoints.update_one(
            {"_id": source_id},
            {"$set": {"last_value": value}},
            upsert=True
        )

    @abstractmethod
    def run(self, *args, **kwargs):
        """Main execution method to be implemented by subclasses."""
        pass

    def close(self):
        self.client.close()
