from src.utils.logger import get_logger
from dotenv import load_dotenv
import duckdb
import os
from src.base import BaseTransformer


load_dotenv()
logger = get_logger("CRYPTO_TRANSFORMER")


class CryptoTransformer(BaseTransformer):
    def __init__(self):
        self.con = duckdb.connect(database='/tmp/duckdb.db')
        self._setup_storage()
        self.bucket = os.getenv("OCI_BUCKET_NAME")
        self.namespace = os.getenv("OCI_NAMESPACE")
    
    def _setup_storage(self):
        """Configures DuckDB to access OCI via S3-compatibility layer."""
        try:
            self.con.execute("LOAD httpfs;")
        except Exception:
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")        
        
        # OCI S3-Compatibility Settings
        region = os.getenv("OCI_REGION")
        namespace = os.getenv("OCI_NAMESPACE")
        endpoint = f"{namespace}.compat.objectstorage.{region}.oraclecloud.com"
        
        self.con.execute(f"""
            CREATE SECRET oci_secret (
                TYPE s3,
                KEY_ID '{os.getenv('OCI_ACCESS_KEY')}',
                SECRET '{os.getenv('OCI_SECRET_KEY')}',
                REGION '{region}',
                ENDPOINT '{endpoint}',
                URL_STYLE 'path',
                USE_SSL true
            );
            """)
        logger.info("DuckDB storage configured for OCI S3 compatibility")
            
    def transform(self, oci_uri: str) -> str:
        """
        Reads raw JSON from OCI, applies analytical transformations,
        and saves the result back to OCI as a Parquet file.
        """

        s3_uri = oci_uri.replace("oci://", "s3://").split("@")[0] + "/" + oci_uri.split("/",3)[-1]
        
        output_uri = s3_uri.replace("/raw/", "/processed/").replace(".json", ".parquet")

        logger.info(f"Processing transformation for: {s3_uri}")

        # Write back to OCI as Parquet
        try:
            # 1. Load and Flatten JSON (UNNESTing the 'data' array)
            self.con.execute(f"""
                CREATE OR REPLACE TABLE raw_data AS
                SELECT UNNEST(data) as asset,
                       epoch_ms(CAST(timestamp AS BIGINT))::TIMESTAMPTZ as event_time,
                       CURRENT_TIMESTAMP::TIMESTAMPTZ as processed_at
                FROM read_json_auto('{s3_uri}');
            """)

            # 2. Analytical Transformation (Window Functions)
            self.con.execute("""
                CREATE OR REPLACE TABLE transformed_data AS
                    WITH base AS (
                        SELECT
                            (asset).id AS asset_id,
                            (asset).symbol AS symbol,
                            (asset).name AS name,

                            TRY_CAST((asset).rank AS INTEGER) AS rank,
                            TRY_CAST((asset).supply AS DOUBLE) AS supply,
                            TRY_CAST((asset).maxSupply AS DOUBLE) AS max_supply,

                            TRY_CAST((asset).priceUsd AS DOUBLE) AS price_usd,
                            TRY_CAST((asset).marketCapUsd AS DOUBLE) AS market_cap_usd,
                            TRY_CAST((asset).volumeUsd24Hr AS DOUBLE) AS volume_usd_24h,

                            TRY_CAST((asset).changePercent24Hr AS DOUBLE) AS change_percent_24h,
                            TRY_CAST((asset).vwap24Hr AS DOUBLE) AS vwap_24h,

                            (asset).explorer AS explorer,

                            event_time,
                            processed_at

                        FROM raw_data
                    )

                    SELECT *,
                        RANK() OVER (
                            ORDER BY change_percent_24h DESC
                        ) AS performance_rank
                    FROM base;
                """)

            # 3. Write to OCI as Parquet
            self.con.execute(f"""
                COPY transformed_data TO '{output_uri}'
                (FORMAT PARQUET, 
                compression 'zstd');
                """)

        except Exception as e:
            logger.error(f"Failed to write transformed data to OCI: {e}")
            raise
        else:
            logger.info(f"Transformation complete. Output: {output_uri}")
            return output_uri

if __name__ == "__main__":
    # The URI you provided from your manual extraction
    test_uri = "oci://jonas-data-platform@axxdt8jrk4om/raw/year=2026/month=03/day=17/assets_20260316_211207.json"
    
    try:
        logger.info("--- Starting Manual Transformation Test ---")
        transformer = CryptoTransformer()
        
        # This will: 
        # 1. Connect to OCI 
        # 2. Run the Window Functions (Rank) 
        # 3. Save a Parquet file in the 'processed/' folder
        output_uri = transformer.transform(test_uri)
        
        print(f"\nSuccess! Refined data saved to: {output_uri}")
        
    except Exception as e:
        print(f"\nTest Failed: {e}")