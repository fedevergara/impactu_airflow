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
        df['year'] = year
        return df.to_dict('records')

    def process_year(self, year, force_redownload=False, chunk_size=1000):
        """Downloads and updates a single year in MongoDB."""
        start_time = time.time()
        try:
            data = self.fetch_year(year, force_redownload=force_redownload)
            if not data:
                self.logger.warning(f"No data found for year {year}")
                return None

            total_records = len(data)
            self.logger.info(f"Processing {total_records} records for year {year}...")
            
            operations = []
            for record in data:
                # Sanitize keys (replace dots) and handle NaN
                sanitized_record = {
                    k.replace('.', '_'): (None if pd.isna(v) else v) 
                    for k, v in record.items()
                }
                
                # Use Sourceid and year as unique identifier
                source_id = sanitized_record.get('Sourceid')
                if source_id:
                    operations.append(
                        UpdateOne(
                            {"Sourceid": source_id, "year": year},
                            {"$set": sanitized_record},
                            upsert=True
                        )
                    )
            
            if operations:
                total_ops = len(operations)
                processed = 0
                
                for i in range(0, total_ops, chunk_size):
                    chunk = operations[i:i + chunk_size]
                    result = self.collection.bulk_write(chunk, ordered=False)
                    processed += len(chunk)
                    self.logger.info(f"Year {year}: Progress {processed}/{total_ops} records ({(processed/total_ops)*100:.1f}%)")
                
                end_time = time.time()
                self.logger.info(
                    f"Year {year} completed in {end_time - start_time:.2f} seconds. "
                    f"Total records processed: {total_ops}"
                )
                self.save_checkpoint(f"scimagojr_{year}", "completed")
                return True
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing year {year}: {e}")
            raise e

    def run(self, start_year, end_year, force_redownload=False, chunk_size=1000):
        """Runs the extraction for a range of years, updating only changed records."""
        for year in range(start_year, end_year + 1):
            self.process_year(year, force_redownload=force_redownload, chunk_size=chunk_size)
            # Respectful delay
            time.sleep(1)
