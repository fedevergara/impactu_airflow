import requests
import pandas as pd
import io
from extract.base_extractor import BaseExtractor
import time
from pymongo import UpdateOne

class ScimagoJRExtractor(BaseExtractor):
    def __init__(self, mongodb_uri, db_name, collection_name="scimagojr", client=None):
        super().__init__(mongodb_uri, db_name, collection_name, client=client)
        self.base_url = "https://www.scimagojr.com/journalrank.php"

    def fetch_year(self, year):
        """Downloads and parses the CSV for a specific year."""
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
        
        # Scimago returns a CSV with semicolon separator even if it says xls
        df = pd.read_csv(io.StringIO(response.text), sep=';', low_memory=False)
        df['year'] = year
        return df.to_dict('records')

    def run(self, start_year, end_year):
        """Runs the extraction for a range of years, updating only changed records."""
        for year in range(start_year, end_year + 1):
            try:
                data = self.fetch_year(year)
                if not data:
                    self.logger.warning(f"No data found for year {year}")
                    continue

                self.logger.info(f"Processing {len(data)} records for year {year}...")
                
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
                    result = self.collection.bulk_write(operations, ordered=False)
                    self.logger.info(
                        f"Year {year}: {result.upserted_count} new, "
                        f"{result.modified_count} changed, "
                        f"{result.matched_count} unchanged."
                    )
                    self.save_checkpoint(f"scimagojr_{year}", "completed")
                
                # Respectful delay
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error processing year {year}: {e}")
                raise e
