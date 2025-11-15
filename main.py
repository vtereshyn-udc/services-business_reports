import argparse
from pathlib import Path

try:
    # from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from services.amazon_ads import AmazonAds
    from services.brand_analytics import BrandAnalytics
    from services.awd import Awd
    from services.fulfillment import Fulfillment
    from services.shipments import Shipments
    from services.support import Support
    from services.business_reports import BusinessReports
    from services.datarova import Datarova
    from services.payments import Payments
    from services.api_ad import AmazonAD
    from services.api_sp import AmazonSP
    from services.brand_analytics_api import BrandAnalyticsAPI
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


@utils.exception
def run():
    parser: argparse.ArgumentParser = argparse.ArgumentParser()

    for argument in config.ARGUMENTS:
        parser.add_argument(argument["flag"], required=argument["required"])

    args: argparse.Namespace = parser.parse_args()
    kwargs: dict = vars(args)

    services: dict = {
        "amazon_ads": AmazonAds,
        "brand_analytics": BrandAnalytics,
        "awd": Awd,
        "fulfillment": Fulfillment,
        "shipments": Shipments,
        "support": Support,
        "business_reports": BusinessReports,
        "datarova": Datarova,
        "payments": Payments,
        "api_ad": AmazonAD,
        "api_sp": AmazonSP,
        "brand_analytics_api": BrandAnalyticsAPI
    }

    service = services[args.service](**kwargs)
    service.run()


if __name__ == "__main__":
    run()
