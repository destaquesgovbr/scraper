import logging
import time
from collections import OrderedDict
from typing import Any, Dict, List

from govbr_scraper.models.monitoring import classify_error
from govbr_scraper.monitoring.structured_log import log_scrape_result, record_scrape_run_safe
from govbr_scraper.scrapers.content_hash import compute_content_hash
from govbr_scraper.scrapers.unique_id import generate_readable_unique_id
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.webscraper import ScrapingError, WebScraper
from govbr_scraper.scrapers.yaml_config import get_config_dir, load_urls_from_yaml

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ScrapeManager:
    """
    A class that focuses on:
      - Loading and filtering URLs from a YAML file.
      - Running web scrapers for the specified agencies and date ranges.
      - Preprocessing, transforming, and preparing raw news data into a well-structured format
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
        Initialize ScrapeManager with a storage backend.

        Args:
            storage: Storage backend (StorageWrapper or DatasetManager)
        """
        self.dataset_manager = storage  # Keep attribute name for compatibility

    def run_scraper(
        self,
        agencies: List[str],
        min_date: str,
        max_date: str,
        sequential: bool,
        allow_update: bool = False,
    ) -> dict:
        """
        Executes the web scraping process for the given agencies, date range,
        and whether the scraping should happen sequentially or in bulk.

        :param agencies: A list of agency names to scrape news from. If None, all agencies are scraped.
        :param min_date: The minimum date for filtering news.
        :param max_date: The maximum date for filtering news.
        :param sequential: Whether to scrape sequentially (True) or in bulk (False).
        :param allow_update: If True, overwrite existing entries in the dataset.
        :return: Dict with metrics: articles_scraped, articles_saved, agencies_processed.
        """
        articles_scraped = 0
        articles_saved = 0
        agencies_processed = []
        errors = []

        try:
            agency_urls = {}
            config_dir = get_config_dir(__file__)
            # Load URLs for each agency in the list
            if agencies:
                for agency in agencies:
                    try:
                        loaded = load_urls_from_yaml(config_dir, "site_urls.yaml", agency)
                        agency_urls.update(loaded)
                    except ValueError as e:
                        errors.append({"agency": agency, "error": str(e)})
                        logging.warning(f"Skipping agency '{agency}': {e}")
            else:
                # Load all agency URLs if agencies list is None or empty
                agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")

            # Create list of (agency_name, scraper) tuples
            # Query known URLs for each agency to enable early stop optimization
            webscrapers = []
            for agency_name, agency_config in agency_urls.items():
                url = agency_config["url"]
                scraper_type = agency_config.get("scraper_type", "html")
                try:
                    known_urls = self.dataset_manager.get_recent_urls(agency_name)
                except Exception:
                    known_urls = set()  # Fallback: no optimization
                # Strategy Pattern: select scraper based on config
                if scraper_type == "plone6_api":
                    scraper = Plone6APIScraper(min_date, url, max_date=max_date, known_urls=known_urls)
                    logging.info(f"Using Plone6APIScraper for {agency_name}")
                else:
                    scraper = WebScraper(min_date, url, max_date=max_date, known_urls=known_urls)
                webscrapers.append((agency_name, scraper))

            if sequential:
                for agency_name, scraper in webscrapers:
                    start_time = time.monotonic()
                    try:
                        scraped_data = scraper.scrape_news()
                        elapsed = time.monotonic() - start_time
                        if scraped_data:
                            logging.info(
                                f"Appending news for {agency_name} to storage backend."
                            )
                            articles_scraped += len(scraped_data)
                            saved = self._process_and_upload_data(scraped_data, allow_update) or 0
                            articles_saved += saved
                            agencies_processed.append(agency_name)
                        else:
                            logging.info(f"No news found for {agency_name}.")
                            agencies_processed.append(agency_name)
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="success",
                            articles_scraped=len(scraped_data) if scraped_data else 0,
                            articles_saved=saved if scraped_data else 0,
                            execution_time_seconds=elapsed,
                        )
                    except ScrapingError as e:
                        elapsed = time.monotonic() - start_time
                        errors.append({"agency": agency_name, "error": str(e)})
                        logging.error(f"Scraping failed for {agency_name}: {e}")
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="error",
                            error_category=classify_error(str(e)),
                            error_message=str(e),
                            execution_time_seconds=elapsed,
                        )
                    except Exception as e:
                        elapsed = time.monotonic() - start_time
                        errors.append({"agency": agency_name, "error": str(e)})
                        logging.error(f"Unexpected error for {agency_name}: {e}")
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="error",
                            error_category=classify_error(str(e)),
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
                    except ScrapingError as e:
                        elapsed = time.monotonic() - start_time
                        errors.append({"agency": agency_name, "error": str(e)})
                        logging.error(f"Scraping failed for {agency_name}: {e}")
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="error",
                            error_category=classify_error(str(e)),
                            error_message=str(e),
                            execution_time_seconds=elapsed,
                        )
                    except Exception as e:
                        elapsed = time.monotonic() - start_time
                        errors.append({"agency": agency_name, "error": str(e)})
                        logging.error(f"Unexpected error for {agency_name}: {e}")
                        run = log_scrape_result(
                            agency_key=agency_name,
                            status="error",
                            error_category=classify_error(str(e)),
                            error_message=str(e),
                            execution_time_seconds=elapsed,
                        )
                    record_scrape_run_safe(self.dataset_manager, run, agency_name)

                if all_news_data:
                    logging.info("Appending all collected news to storage backend.")
                    articles_scraped = len(all_news_data)
                    articles_saved = self._process_and_upload_data(all_news_data, allow_update) or 0
                else:
                    logging.info("No news found for any agency.")
        except ValueError as e:
            logging.error(e)
            errors.append({"agency": "config", "error": str(e)})

        return {
            "articles_scraped": articles_scraped,
            "articles_saved": articles_saved,
            "agencies_processed": agencies_processed,
            "errors": errors,
        }

    def _process_and_upload_data(self, new_data, allow_update: bool):
        """
        Process the news data and upload it to the dataset, with the option to update existing entries.

        :param new_data: The list of news items to process.
        :param allow_update: If True, overwrite existing entries in the dataset.
        """
        new_data = self._preprocess_data(new_data)
        return self.dataset_manager.insert(new_data, allow_update=allow_update)

    def _preprocess_data(self, data: List[Dict[str, str]]) -> OrderedDict:
        """
        Preprocess data by:
        - Adding the unique_id column.
        - Reordering columns.

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
        column_data = {
            key: [item.get(key, None) for item in data] for key in data[0].keys()
        }

        # Reorder columns
        ordered_column_data = OrderedDict()
        if "unique_id" in column_data:
            ordered_column_data["unique_id"] = column_data.pop("unique_id")
        if "agency" in column_data:
            ordered_column_data["agency"] = column_data.pop("agency")
        if "published_at" in column_data:
            ordered_column_data["published_at"] = column_data.pop("published_at")
        if "updated_datetime" in column_data:
            ordered_column_data["updated_datetime"] = column_data.pop("updated_datetime")
        if "title" in column_data:
            ordered_column_data["title"] = column_data.pop("title")
        if "editorial_lead" in column_data:
            ordered_column_data["editorial_lead"] = column_data.pop("editorial_lead")
        if "subtitle" in column_data:
            ordered_column_data["subtitle"] = column_data.pop("subtitle")
        ordered_column_data.update(column_data)

        return ordered_column_data

    def _generate_unique_id(
        self, agency: str, published_at_value: str, title: str
    ) -> str:
        """
        Generate a unique identifier based on the agency, published_at, and title.

        :param agency: The agency name.
        :param published_at_value: The published_at date of the news item (string format or datetime.date).
        :param title: The title of the news item.
        :return: A readable slug with hash suffix (e.g., "governo-anuncia-programa_a3f2e1").
        """
        return generate_readable_unique_id(agency, published_at_value, title)
