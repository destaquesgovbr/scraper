import logging
import time
from collections import OrderedDict
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from govbr_scraper.models.monitoring import (
    UNKNOWN_AGENCY,
    agency_error,
    classify_exception,
)
from govbr_scraper.monitoring.structured_log import log_scrape_result, record_scrape_run_safe
from govbr_scraper.scrapers.content_hash import compute_content_hash
from govbr_scraper.scrapers.ebc_webscraper import EBCWebScraper
from govbr_scraper.scrapers.unique_id import generate_readable_unique_id
from govbr_scraper.scrapers.yaml_config import (
    UnknownAgencyError,
    get_config_dir,
    load_urls_from_yaml,
)

if TYPE_CHECKING:
    from govbr_scraper.storage.storage_adapter import InsertCounts

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class EBCScrapeManager:
    """
    A class that focuses on:
      - Loading and filtering URLs from a YAML file.
      - Running EBC web scrapers for the specified date ranges.
      - Preprocessing, transforming, and preparing raw EBC news data into a well-structured format
        ready for dataset creation and analysis.
      - Generating unique identifiers for news items based on their attributes (agency,
        published date, and title).
      - Converting raw data from a list-of-dictionaries format into a columnar (OrderedDict) format.
      - Merging new data with an existing dataset, ensuring no duplicates by comparing unique IDs.
      - Sorting the combined dataset by specified criteria (e.g., agency and publication date).
      - Preparing the final processed data into columnar format suitable for integration with
        a dataset manager.
    """

    def __init__(self, storage: Any):
        """
        Initialize EBCScrapeManager with a storage backend.

        Args:
            storage: Storage backend (StorageWrapper or DatasetManager)
        """
        self.dataset_manager = storage  # Keep attribute name for compatibility

    def run_scraper(
        self,
        min_date: str,
        max_date: str,
        sequential: bool,
        allow_update: bool = False,
        agencies: list[str] = None,
    ) -> dict:
        """
        Executes the EBC web scraping process for the given date range.

        :param min_date: The minimum date for filtering news.
        :param max_date: The maximum date for filtering news.
        :param sequential: Whether to scrape sequentially (True) or in bulk (False).
        :param allow_update: If True, overwrite existing entries in the dataset.
        :param agencies: A list of agency names to scrape news from. If None, all active agencies are scraped.
        :return: Dict with metrics: articles_scraped, articles_saved (inserted + updated),
            articles_new (inserted only), agencies_processed and errors (agency, error,
            category, retryable; category is the one written to scrape_runs).
        """
        articles_scraped = 0
        articles_saved = 0
        articles_new = 0
        agencies_processed = []
        errors = []

        try:
            agency_urls = {}
            config_dir = get_config_dir(__file__)
            # Load URLs for each agency in the list
            if agencies:
                for agency in agencies:
                    try:
                        loaded = load_urls_from_yaml(config_dir, "ebc_urls.yaml", agency)
                        agency_urls.update(loaded)
                    except UnknownAgencyError as e:
                        errors.append(agency_error(agency, str(e), UNKNOWN_AGENCY))
                        logging.warning(f"Skipping agency '{agency}': {e}")
            else:
                # Load all agency URLs if agencies list is None or empty
                agency_urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml")

            # Create list of (agency_name, scraper) tuples
            webscrapers = [
                (agency_name, EBCWebScraper(min_date, agency_config["url"], max_date=max_date))
                for agency_name, agency_config in agency_urls.items()
            ]

            if sequential:
                for agency_name, scraper in webscrapers:
                    start_time = time.monotonic()
                    try:
                        scraped_data = scraper.scrape_news()
                        elapsed = time.monotonic() - start_time
                        saved = new = 0
                        if scraped_data:
                            logging.info(
                                f"Appending {len(scraped_data)} news from {agency_name} to dataset."
                            )
                            articles_scraped += len(scraped_data)
                            counts = self._process_and_upload_data(scraped_data, allow_update)
                            saved, new = counts.saved, counts.new
                            articles_saved += saved
                            articles_new += new
                            agencies_processed.append(agency_name)
                        else:
                            logging.info(f"No news found for {agency_name}.")
                            agencies_processed.append(agency_name)
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="success",
                            articles_scraped=len(scraped_data) if scraped_data else 0,
                            articles_saved=saved,
                            articles_new=new,
                            execution_time_seconds=elapsed,
                        )
                    except Exception as e:
                        elapsed = time.monotonic() - start_time
                        category = classify_exception(e)
                        errors.append(agency_error(agency_name, str(e), category))
                        logging.error(f"Error scraping {agency_name}: {e}")
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="error",
                            error_category=category,
                            error_message=str(e),
                            execution_time_seconds=elapsed,
                        )
                    record_scrape_run_safe(self.dataset_manager, run, agency_name)
            else:
                all_news_data = []
                for agency_name, scraper in webscrapers:
                    start_time = time.monotonic()
                    try:
                        scraped_data = scraper.scrape_news()
                        elapsed = time.monotonic() - start_time
                        if scraped_data:
                            all_news_data.extend(scraped_data)
                            agencies_processed.append(agency_name)
                        else:
                            logging.info(f"No news found for {agency_name}.")
                            agencies_processed.append(agency_name)
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="success",
                            articles_scraped=len(scraped_data) if scraped_data else 0,
                            articles_saved=len(scraped_data) if scraped_data else 0,
                            execution_time_seconds=elapsed,
                        )
                    except Exception as e:
                        elapsed = time.monotonic() - start_time
                        category = classify_exception(e)
                        errors.append(agency_error(agency_name, str(e), category))
                        logging.error(f"Error scraping {agency_name}: {e}")
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="error",
                            error_category=category,
                            error_message=str(e),
                            execution_time_seconds=elapsed,
                        )
                    record_scrape_run_safe(self.dataset_manager, run, agency_name)

                if all_news_data:
                    # Gravação única no fim: scrape_runs de cada fonte fica sem articles_new
                    # (None); a resposta da API leva o total do lote.
                    logging.info("Appending all collected news to dataset.")
                    articles_scraped = len(all_news_data)
                    counts = self._process_and_upload_data(all_news_data, allow_update)
                    articles_saved = counts.saved
                    articles_new = counts.new
                else:
                    logging.info("No news found for any EBC source.")
        except ValueError as e:
            logging.error(e)
            errors.append(agency_error("config", str(e), None))

        return {
            "articles_scraped": articles_scraped,
            "articles_saved": articles_saved,
            "articles_new": articles_new,
            "agencies_processed": agencies_processed,
            "errors": errors,
        }

    def _process_and_upload_data(self, new_data: list[dict], allow_update: bool) -> "InsertCounts":
        """
        Process the EBC news data and upload it to the dataset, with the option to update existing entries.

        :param new_data: The list of EBC news items to process.
        :param allow_update: If True, overwrite existing entries in the dataset.
        :return: InsertCounts(saved, new) from the storage backend.
        """
        # Convert EBC data format to govbrnews format
        processed_data = self._convert_ebc_to_govbr_format(new_data)

        # Preprocess the data (add unique IDs, reorder columns)
        processed_data = self._preprocess_data(processed_data)

        # Insert into dataset
        return self.dataset_manager.insert(processed_data, allow_update=allow_update)

    def _convert_ebc_to_govbr_format(self, ebc_data: list[dict]) -> list[dict]:
        """
        Convert EBC data format to match the govbrnews schema.

        :param ebc_data: List of EBC news items as dictionaries.
        :return: List of news items in govbrnews format.
        """
        converted_data = []

        for item in ebc_data:
            # Skip items with errors
            if item.get("error"):
                logging.warning(f"Skipping item with error: {item['error']}")
                continue

            # Get datetimes (already extracted by EBCWebScraper)
            published_dt = item.get("published_datetime")
            updated_datetime = item.get("updated_datetime")

            # Use the agency from the scraped data (either 'agencia_brasil' or 'tvbrasil')
            # Fallback to 'ebc' if not specified
            agency = item.get("agency", "ebc")

            # Extract editorial_lead (e.g., program name for TV Brasil)
            editorial_lead = item.get("editorial_lead", "").strip() or None

            converted_item = {
                "title": item.get("title", "").strip(),
                "url": item.get("url", "").strip(),
                "published_at": published_dt if published_dt else None,
                "updated_datetime": updated_datetime,
                "category": "Notícias",  # EBC doesn't have specific categories like gov.br
                "tags": item.get("tags", []),  # Now extracted from article pages
                "editorial_lead": editorial_lead,  # Program name for TV Brasil (e.g., "Caminhos da Reportagem")
                "subtitle": None,  # EBC articles don't typically have subtitles in the same format
                "content": item.get("content", "").strip(),
                "image": item.get("image", "").strip(),
                "video_url": item.get("video_url", "").strip(),
                "agency": agency,
                "extracted_at": datetime.now(UTC),
            }

            # Only add items with essential data
            if converted_item["title"] and converted_item["url"] and converted_item["content"]:
                converted_data.append(converted_item)
            else:
                logging.warning(f"Skipping incomplete item: {item.get('url', 'unknown URL')}")

        return converted_data

    def _parse_ebc_date(self, date_str: str) -> date:
        """
        Parse EBC date string to date object.
        Expected format: "16/09/2025 - 13:40"

        :param date_str: Date string from EBC.
        :return: Date object or current date if parsing fails.
        """
        try:
            if not date_str:
                return datetime.now().date()

            # Remove extra whitespace and split by ' - '
            date_part = date_str.strip().split(" - ")[0]
            # Parse the date part (DD/MM/YYYY)
            return datetime.strptime(date_part, "%d/%m/%Y").date()
        except Exception as e:
            logging.warning(f"Could not parse date '{date_str}': {e}. Using current date.")
            return datetime.now().date()

    def _preprocess_data(self, data: list[dict[str, str]]) -> OrderedDict:
        """
        Preprocess data by:
        - Adding the unique_id column.
        - Reordering columns to match govbrnews format.

        :param data: List of news items as dictionaries.
        :return: An OrderedDict with the processed data.
        """
        # Generate unique_id for each record
        for item in data:
            item["unique_id"] = self._generate_unique_id(
                item.get("agency", ""),
                item.get("published_at", ""),
                item.get("title", ""),
            )
            item["content_hash"] = compute_content_hash(
                item.get("title", ""),
                item.get("content"),
            )

        # Convert to columnar format
        if not data:
            return OrderedDict()

        column_data = {key: [item.get(key, None) for item in data] for key in data[0].keys()}

        # Reorder columns to match govbrnews format
        ordered_column_data = OrderedDict()
        if "unique_id" in column_data:
            ordered_column_data["unique_id"] = column_data.pop("unique_id")
        if "agency" in column_data:
            ordered_column_data["agency"] = column_data.pop("agency")
        if "published_at" in column_data:
            ordered_column_data["published_at"] = column_data.pop("published_at")
        if "updated_datetime" in column_data:
            ordered_column_data["updated_datetime"] = column_data.pop("updated_datetime")

        # Add remaining columns in order (matching govbrnews schema)
        for key in [
            "title",
            "editorial_lead",
            "subtitle",
            "url",
            "category",
            "tags",
            "content",
            "image",
            "video_url",
            "extracted_at",
        ]:
            if key in column_data:
                ordered_column_data[key] = column_data.pop(key)

        # Add any remaining columns
        ordered_column_data.update(column_data)

        return ordered_column_data

    def _generate_unique_id(self, agency: str, published_at_value, title: str) -> str:
        """
        Generate a unique identifier based on the agency, published_at, and title.

        :param agency: The agency name.
        :param published_at_value: The published_at date of the news item (string format or date object).
        :param title: The title of the news item.
        :return: A readable slug with hash suffix (e.g., "governo-anuncia-programa_a3f2e1").
        """
        return generate_readable_unique_id(agency, published_at_value, title)
