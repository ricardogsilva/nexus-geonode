import logging
import typing

import requests

logger = logging.getLogger(__name__)


class PostgRestClient:
    api_base_url: str
    http_session: requests.Session
    page_size: int
    request_timeout_seconds: int

    def __init__(
            self,
            api_base_url: str,
            page_size: typing.Optional[int] = 10,
            timeout: typing.Optional[int] = 5
    ) -> None:
        """Harvester-minded client for remote resources that use PostgREST as their API engine"""
        self.http_session = requests.Session()
        self.request_timeout_seconds = timeout
        self.api_base_url = api_base_url
        self.page_size = page_size

    def check_availability(self) -> bool:
        response = self.http_session.get(self.api_base_url, timeout=self.request_timeout_seconds)
        return response.status_code == requests.codes.ok

    def get_total_records(self, endpoint: str) -> int:
        url = f"{self.api_base_url}{endpoint}"
        logger.info(f"url: {url}")
        response = self.http_session.get(
            url,
            headers={
                "Range-Unit": "items",
                "Range": "0-0",
                "Prefer": "count=exact",
            },
            timeout=self.request_timeout_seconds
        )
        if response.status_code == requests.codes.partial_content:
            content_range = response.headers["Content-Range"]
            result = int(content_range.rpartition("/")[-1])
        else:
            logger.error(
                f"Got back invalid response from {url!r} when determining number of "
                f"total records"
            )
            result = 0
        return result

    def get_paginated_resources(
            self,
            endpoint: str,
            offset: typing.Optional[int] = 0
    ) -> typing.List[typing.Dict]:
        url = f"{self.api_base_url}{endpoint}"
        limit = offset + (self.page_size - 1)
        response = self.http_session.get(
            url,
            headers={
                "Range-Unit": "items",
                "Range": f"{offset}-{limit}",
                "Accept": "application/json",
            },
            timeout=self.request_timeout_seconds
        )
        if response.status_code == requests.codes.ok:
            result = response.json()
        else:
            logger.error(f"Received invalid response from {url}: {response.status_code} - {response.reason}")
            result = []
        return result

    def get_resource(
            self,
            endpoint: str,
            unique_identifier: str,
            unique_identifier_name: typing.Optional[str] = "id"
    ) -> typing.Optional[typing.Dict]:
        url = f"{self.api_base_url}{endpoint}"
        response = self.http_session.get(
            url,
            params={
                unique_identifier_name: f"eq.{unique_identifier}",
            },
            headers={
                "Accept": "application/vnd.pgrst.object+json",
            },
            timeout=self.request_timeout_seconds
        )
        if response.status_code == requests.codes.ok:
            result = response.json()
        else:
            result = None
            logger.error(
                f"Could not retrieve resource with {unique_identifier_name}={unique_identifier}: "
                f"{response.status_code} - {response.reason}"
            )
        return result