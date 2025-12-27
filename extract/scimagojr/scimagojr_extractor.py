import requests
import pandas as pd
import io
import os
from extract.base_extractor import BaseExtractor
import time
from pymongo import UpdateOne

class ScimagoJRExtractor(BaseExtractor):
    def __init__(self, mongodb_uri, db_name, collection_name="scimagojr", client=None, cache_dir="/opt/airflow/impactu/cache/scimagojr"):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)
        self.base_url = "https://www.scimagojr.com/journalrank.php"
        self.cache_dir = cache_dir
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)

    def fetch_year(self, year, force_redownload=False):
        """Downloads and parses the CSV for a specific year, using cache if available."""
        cache_file = os.path.join(self.cache_dir, f"scimagojr_{year}.csv")
        
        # Check if we should use cache: not forcing redownload and file exists and is not empty
        if not force_redownload and os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
            self.logger.info(f"Loading data for year {year} from cache...")
            with open(cache_file, 'r', encoding='utf-8') as f:
                content = f.read()
        else:
            # If file is missing from cache or force_redownload is True, we download it.
            # We no longer skip download if data exists in MongoDB, to allow for updates/completion.
            params = {
                "year": year,
                "type": "all",
                "out": "xls"
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.scimagojr.com/journalrank.php"
            }
            self.logger.info(f"Downloading data for year {year}...")
            
            response = requests.get(self.base_url, params=params, headers=headers)
            response.raise_for_status()
            content = response.text
            
            # Save to cache
            self.logger.info(f"Saving year {year} to cache at: {os.path.abspath(cache_file)}")
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
        
        # Scimago returns a CSV with semicolon separator even if it says xls
        df = pd.read_csv(io.StringIO(content), sep=';', low_memory=False)
        
        # Ensure Sourceid is consistent and unique
        if 'Sourceid' in df.columns:
            df['Sourceid'] = pd.to_numeric(df['Sourceid'], errors='coerce').fillna(0).astype(int)
            df = df.drop_duplicates(subset=['Sourceid'])
        
        df['year'] = int(year)
        
        # Optimization: Sanitize columns and handle NaN using Pandas
        df.columns = [c.replace('.', '_') for c in df.columns]
        df = df.where(pd.notnull(df), None)
        
        return df.to_dict('records')

    def process_year(self, year, force_redownload=False, chunk_size=1000):
        """Downloads and updates a single year in MongoDB."""
        start_time = time.time()
        try:
            fetch_start = time.time()
            data = self.fetch_year(year, force_redownload=force_redownload)
            fetch_end = time.time()
            
            if not data:
                self.logger.warning(f"No data found for year {year}")
                return None

            total_records = len(data)
            self.logger.info(f"Processing {total_records} records for year {year}...")
            
            sanitization_start = time.time()
            operations = []
            for record in data:
                # Use Sourceid and year as unique identifier
                source_id = record.get('Sourceid')
                if source_id:
                    operations.append(
                        UpdateOne(
                            {"Sourceid": source_id, "year": year},
                            {"$set": record},
                            upsert=True
                        )
                    )
            sanitization_end = time.time()
            
            if operations:
                total_ops = len(operations)
                processed = 0
                bulk_write_total_time = 0
                
                for i in range(0, total_ops, chunk_size):
                    chunk = operations[i:i + chunk_size]
                    bw_start = time.time()
                    result = self.collection.bulk_write(chunk, ordered=False)
                    bw_end = time.time()
                    bulk_write_total_time += (bw_end - bw_start)
                    
                    processed += len(chunk)
                    self.logger.info(f"Year {year}: Progress {processed}/{total_ops} records ({(processed/total_ops)*100:.1f}%)")
                
                end_time = time.time()
                self.logger.info(
                    f"Year {year} performance breakdown:\n"
                    f"  - Fetching: {fetch_end - fetch_start:.2f}s\n"
                    f"  - Sanitization: {sanitization_end - sanitization_start:.2f}s\n"
                    f"  - Bulk Write (Total): {bulk_write_total_time:.2f}s\n"
                    f"  - Total Time: {end_time - start_time:.2f}s"
                )
                
                # Cleanup duplicates if count doesn't match
                self.cleanup_year(year, total_records)
                
                self.save_checkpoint(f"scimagojr_{year}", "completed")
                return True
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing year {year}: {e}")
            raise e

    def cleanup_year(self, year, expected_count):
        """Checks if the number of records in MongoDB matches the expected count and cleans up duplicates if necessary."""
        actual_count = self.collection.count_documents({"year": year})
        if actual_count > expected_count:
            self.logger.info(f"Year {year}: Found {actual_count} records, expected {expected_count}. Cleaning up duplicates...")
            pipeline = [
                {"$match": {"year": year}},
                {"$group": {
                    "_id": "$Sourceid",
                    "dups": {"$push": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {"count": {"$gt": 1}}}
            ]
            duplicates = list(self.collection.aggregate(pipeline))
            for doc in duplicates:
                # Keep the first one, delete the rest
                ids_to_delete = doc['dups'][1:]
                self.collection.delete_many({"_id": {"$in": ids_to_delete}})
            
            new_count = self.collection.count_documents({"year": year})
            self.logger.info(f"Year {year}: Cleanup finished. New count: {new_count}")
        elif actual_count == expected_count:
            self.logger.info(f"Year {year}: Count matches expected ({expected_count}). No cleanup needed.")
        else:
            self.logger.warning(f"Year {year}: Found fewer records ({actual_count}) than expected ({expected_count}).")

    def run(self, start_year, end_year, force_redownload=False, chunk_size=1000):
        """Runs the extraction for a range of years, updating only changed records."""
        for year in range(start_year, end_year + 1):
            self.process_year(year, force_redownload=force_redownload, chunk_size=chunk_size)
            # Respectful delay
            time.sleep(1)
