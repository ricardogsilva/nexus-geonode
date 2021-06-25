import enum
import logging
import typing

import dateutil.parser
import requests
from geonode.base.models import ResourceBase
from geonode.harvesting import (
    models as harvesting_models,
    resourcedescriptor
)
from geonode.harvesting.harvesters import base

from . import models

logger = logging.getLogger(__name__)


class PdnResourceType(enum.Enum):
    ALERT = "alert"
    DOCUMENT = "document"
    EXPERT = "experts"
    NEWS_ARTICLE = "news"
    PROJECT = "project"


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
        response = self.http_session.get(
            url,
            headers={
                "Range-Unit": "items",
                "Range": f"{offset}-{self.page_size - 1}",
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


class PdnHarvesterWorker(base.BaseHarvesterWorker):
    _api_client: PostgRestClient
    _UNIQUE_ID_SEPARATOR: typing.Final = "-"
    harvest_alerts: bool
    harvest_documents: bool
    harvest_experts: bool
    harvest_news: bool
    harvest_projects: bool

    def __init__(
            self,
            *args,
            harvest_alerts: typing.Optional[bool] = True,
            harvest_documents: typing.Optional[bool] = True,
            harvest_experts: typing.Optional[bool] = True,
            harvest_news: typing.Optional[bool] = True,
            harvest_projects: typing.Optional[bool] = True,
            page_size: typing.Optional[int] = 10,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self._api_client = PostgRestClient(self.base_api_url, page_size=page_size)
        self.harvest_alerts = harvest_alerts
        self.harvest_documents = harvest_documents
        self.harvest_experts = harvest_experts
        self.harvest_news = harvest_news
        self.harvest_projects = harvest_projects

    @property
    def base_api_url(self):
        return f"{self.remote_url}/api"

    @property
    def allows_copying_resources(self) -> bool:
        return False

    @classmethod
    def from_django_record(cls, harvester: "Harvester"):
        """Return a new instance of the worker from the django harvester"""
        return cls(
            harvester.remote_url,
            harvester.id,
            harvest_alerts=harvester.harvester_type_specific_configuration.get(
                "harvest_alerts", True),
            harvest_documents=harvester.harvester_type_specific_configuration.get(
                "harvest_documents", True),
            harvest_experts=harvester.harvester_type_specific_configuration.get(
                "harvest_experts", True),
            harvest_news=harvester.harvester_type_specific_configuration.get(
                "harvest_news", True),
            harvest_projects=harvester.harvester_type_specific_configuration.get(
                "harvest_projects", True),
        )

    @classmethod
    def get_extra_config_schema(cls) -> typing.Dict:
        return {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": (
                "https://nexus.psc.int/harvesting/pdn-harvester.schema.json"),
            "title": "PDN harvester config",
            "description": (
                "A jsonschema for validating configuration option for GeoNode's "
                "remote PDN harvester"
            ),
            "type": "object",
            "properties": {
                "harvest_alerts": {
                    "type": "boolean",
                    "default": True
                },
                "harvest_documents": {
                    "type": "boolean",
                    "default": True
                },
                "harvest_experts": {
                    "type": "boolean",
                    "default": True
                },
                "harvest_news": {
                    "type": "boolean",
                    "default": True
                },
                "harvest_projects": {
                    "type": "boolean",
                    "default": True
                },
            },
            "additionalProperties": False,
        }

    def get_num_available_resources(self) -> int:
        """Return the number of available resources on the remote service"""
        return sum(self._get_num_available_resources_by_type().values())

    def list_resources(
            self,
            offset: typing.Optional[int] = 0
    ) -> typing.List[base.BriefRemoteResource]:
        """Return a list of resources from the remote service"""
        # The implementation of this method is a bit convoluted because PDN
        # does not have a common `/resources` endpoint, we must query
        # the individual endpoints for alter, news, etc and work out the
        # correct offsets to use.
        # NOTE: resource types are checked in alphabetical order: alert - document - expert - news - projects

        total_resources = self._get_num_available_resources_by_type()
        if offset < total_resources[PdnResourceType.ALERT]:
            result = self._list_resources_starting_from_alerts(offset)
        elif offset < (
            total_resources[PdnResourceType.ALERT] +
            total_resources[PdnResourceType.DOCUMENT]
        ):
            documents_offset = offset - total_resources[PdnResourceType.ALERT]
            result = self._list_resources_starting_from_documents(documents_offset)
        elif offset < (
            total_resources[PdnResourceType.ALERT] +
            total_resources[PdnResourceType.DOCUMENT] +
            total_resources[PdnResourceType.EXPERT]
        ):
            experts_offset = offset - (
                    total_resources[PdnResourceType.ALERT] +
                    total_resources[PdnResourceType.DOCUMENT]
            )
            result = self._list_resources_starting_from_experts(experts_offset)
        elif offset < (
                total_resources[PdnResourceType.ALERT] +
                total_resources[PdnResourceType.DOCUMENT] +
                total_resources[PdnResourceType.EXPERT] +
                total_resources[PdnResourceType.NEWS_ARTICLE]
        ):
            news_offset = offset - (
                total_resources[PdnResourceType.ALERT] +
                total_resources[PdnResourceType.DOCUMENT] +
                total_resources[PdnResourceType.EXPERT]
            )
            result = self._list_resources_starting_from_news(news_offset)
        else:
            projects_offset = offset - (
                total_resources[PdnResourceType.ALERT] +
                total_resources[PdnResourceType.DOCUMENT] +
                total_resources[PdnResourceType.EXPERT] +
                total_resources[PdnResourceType.NEWS_ARTICLE]
            )
            _page = self._api_client.page_size
            result = self._list_brief_resources(PdnResourceType.PROJECT, projects_offset)[:_page]
        return result

    def check_availability(self, timeout_seconds: typing.Optional[int] = 5) -> bool:
        """Check whether the remote service is online"""
        return self._api_client.check_availability()

    def get_geonode_resource_type(self, remote_resource_type: str) -> ResourceBase:
        """
        Return the GeoNode type that should be created from the remote resource type
        """
        raise NotImplementedError

    def get_resource(
            self,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> typing.Optional[resourcedescriptor.RecordDescription]:
        """Harvest a single resource from the remote service"""
        remote_id = harvestable_resource.rpartition(self._UNIQUE_ID_SEPARATOR)[-1]
        raw_resource = self._api_client.get_resource(harvestable_resource.remote_resource_type, remote_id)
        result = None
        if raw_resource is not None:
            if harvestable_resource.remote_resource_type == PdnResourceType.DOCUMENT.value:
                # raise NotImplementedError
                resource_descriptor = self._get_resource_descriptor_for_document_resource(raw_resource)
                result = base.HarvestedResourceInfo(
                    resource_descriptor=resource_descriptor,
                    additional_information=None
                )
            else:
                result = base.HarvestedResourceInfo(
                    resource_descriptor=None,
                    additional_information=raw_resource
                )
        return result

    def update_geonode_resource(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int,
    ):
        handler = {
            PdnResourceType.ALERT: self._update_alert_record,
            PdnResourceType.DOCUMENT: self._update_document_record,
            PdnResourceType.EXPERT: self._update_expert_record,
            PdnResourceType.NEWS_ARTICLE: self._update_news_record,
            PdnResourceType.PROJECT: self._update_project_record,
        }.get(PdnResourceType(harvestable_resource.remote_resource_type))
        if handler is not None:
            return handler(harvested_info, harvestable_resource, harvesting_session_id)
        else:
            raise RuntimeError(f"Invalid resource type: {harvestable_resource.remote_resource_type}")

    def _update_alert_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ):
        raw_record: typing.Dict = harvested_info.additional_information
        try:
            date_received = dateutil.parser.parse(raw_record["datereceived"])
        except KeyError:
            date_received = None
        instance, created = models.Alert.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "content": raw_record.get("content", ""),
                "countries": raw_record.get("countries", ""),
                "datereceived": date_received,
                "ignore": raw_record.get("ignore", False),
                "subject": raw_record.get("subject", ""),
                "logo_url": raw_record.get("logo_url", ""),  # TODO: This field seems to be absent from the remote API
                "uuid": raw_record.get("uuid", ""),
                "active": raw_record.get("active", True),  # TODO: This field seems to be absent from the remote API
                "source_id": raw_record.get("source_id", 0),
            }
        )

    def _update_document_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ):
        pass

    def _update_expert_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ):
        raw_record: typing.Dict = harvested_info.additional_information
        instance, created = models.Expert.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "name": raw_record.get("name", ""),
                "title": raw_record.get("title", ""),
                "country": raw_record.get("country", ""),
                "country_code": raw_record.get("country_code", ""),
                "email": raw_record.get("email", ""),
                "ministry": raw_record.get("ministry", ""),
                "country_id": raw_record.get("country_id", ""),
            }
        )

    def _update_news_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ):
        raw_record: typing.Dict = harvested_info.additional_information
        try:
            date_ = dateutil.parser.parse(raw_record["date"])
        except KeyError:
            date_ = None
        instance, created = models.Alert.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "source_id": raw_record.get("source_id", 0),
                "title": raw_record.get("title", ""),
                "url": raw_record.get("url", ""),
                "country": raw_record.get("country", ""),
                "country_code": raw_record.get("country_code", ""),
                "date": date_,
                "source": raw_record.get("source", ""),
            }
        )

    def _update_project_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ):
        raw_record: typing.Dict = harvested_info.additional_information
        instance, created = models.Alert.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "name": raw_record.get("name", ""),
                "acronym": raw_record.get("acronym", ""),
                "description": raw_record.get("description", ""),
                "logo_url": raw_record.get("logo_url", ""),
                "url": raw_record.get("url", ""),
                "active": raw_record.get("active", False),
            }
        )

    def _list_resources_starting_from_alerts(self, offset: int) -> typing.List[base.BriefRemoteResource]:
        _page = self._api_client.page_size
        alert_list = self._list_brief_resources(PdnResourceType.ALERT, offset)
        if len(alert_list) < _page:
            document_list = self._list_brief_resources(PdnResourceType.DOCUMENT, 0)
            added = alert_list + document_list
            if len(added) < _page:
                expert_list = self._list_brief_resources(PdnResourceType.EXPERT, 0)
                added += expert_list
                if len(added) < _page:
                    news_list = self._list_brief_resources(PdnResourceType.NEWS_ARTICLE, 0)
                    added += news_list
                    if len(added) < _page:
                        projects_list = self._list_brief_resources(PdnResourceType.PROJECT, 0)
                        result = (added + projects_list)[:_page]
                    else:
                        result = added[:_page]
                else:
                    result = added[:_page]
            else:
                result = added[:_page]
        else:
            result = alert_list[:_page]
        return result

    def _list_resources_starting_from_documents(self, offset: int) -> typing.List[base.BriefRemoteResource]:
        _page = self._api_client.page_size
        document_list = self._list_brief_resources(PdnResourceType.DOCUMENT, offset)
        if len(document_list) < _page:
            expert_list = self._list_brief_resources(PdnResourceType.EXPERT, 0)
            added = document_list + expert_list
            if len(added) < _page:
                news_list = self._list_brief_resources(PdnResourceType.NEWS_ARTICLE, 0)
                added += news_list
                if len(added) < _page:
                    projects_list = self._list_brief_resources(PdnResourceType.PROJECT, 0)
                    result = (added + projects_list)[:_page]
                else:
                    result = added[:_page]
            else:
                result = added[:_page]
        else:
            result = document_list[:_page]
        return result

    def _list_resources_starting_from_experts(self, offset: int) -> typing.List[base.BriefRemoteResource]:
        _page = self._api_client.page_size
        expert_list = self._list_brief_resources(PdnResourceType.EXPERT, offset)
        if len(expert_list) < _page:
            news_list = self._list_brief_resources(PdnResourceType.NEWS_ARTICLE, 0)
            added = expert_list + news_list
            if len(added) < _page:
                projects_list = self._list_brief_resources(PdnResourceType.PROJECT, 0)
                result = (added + projects_list)[:_page]
            else:
                result = added[:_page]
        else:
            result = expert_list[:_page]
        return result

    def _list_resources_starting_from_news(self, offset: int) -> typing.List[base.BriefRemoteResource]:
        _page = self._api_client.page_size
        news_list = self._list_brief_resources(PdnResourceType.NEWS_ARTICLE, offset)
        if len(news_list) < _page:
            projects_list = self._list_brief_resources(PdnResourceType.PROJECT, 0)
            result = (news_list + projects_list)[:_page]
        else:
            result = news_list[:_page]
        return result

    def _get_num_available_resources_by_type(self):
        result = {
            PdnResourceType.ALERT: 0,
            PdnResourceType.DOCUMENT: 0,
            PdnResourceType.EXPERT: 0,
            PdnResourceType.NEWS_ARTICLE: 0,
            PdnResourceType.PROJECT: 0,
        }
        if self.harvest_alerts:
            result[PdnResourceType.ALERT] = self._api_client.get_total_records(f"/{PdnResourceType.ALERT.value}")
        if self.harvest_documents:
            result[PdnResourceType.DOCUMENT] = self._api_client.get_total_records(f"/{PdnResourceType.DOCUMENT.value}")
        if self.harvest_experts:
            result[PdnResourceType.EXPERT] = self._api_client.get_total_records(f"/{PdnResourceType.EXPERT.value}")
        if self.harvest_news:
            result[PdnResourceType.NEWS_ARTICLE] = self._api_client.get_total_records(f"/{PdnResourceType.NEWS_ARTICLE.value}")
        if self.harvest_projects:
            result[PdnResourceType.PROJECT] = self._api_client.get_total_records(f"/{PdnResourceType.PROJECT.value}")
        return result

    def _list_brief_resources(self, resource_type: PdnResourceType, offset: int):
        should_list = {
            PdnResourceType.ALERT: self.harvest_alerts,
            PdnResourceType.DOCUMENT: self.harvest_documents,
            PdnResourceType.EXPERT: self.harvest_experts,
            PdnResourceType.NEWS_ARTICLE: self.harvest_news,
            PdnResourceType.PROJECT: self.harvest_projects,
        }[resource_type]
        result = []
        if should_list:
            raw_result = self._api_client.get_paginated_resources(f"/{resource_type.value}", offset)
            for record in raw_result:
                if resource_type == PdnResourceType.ALERT:
                    title = f"{record['subject']} - {record.get('daterecieved', '')}"
                elif resource_type == PdnResourceType.EXPERT:
                    title = f"{record['name']} - {record['title']}"
                elif resource_type == PdnResourceType.NEWS_ARTICLE:
                    title = record["title"]
                elif resource_type == PdnResourceType.PROJECT:
                    title = f"{record['acronym']} - {record['name']}"
                else:
                    raise RuntimeError(f"Invalid resource type: {resource_type}")
                result.append(
                    base.BriefRemoteResource(
                        unique_identifier=f"{resource_type.value}{self._UNIQUE_ID_SEPARATOR}{record['id']}",
                        title=title,
                        resource_type=resource_type.value,
                    )
                )
        return result

    def _get_resource_descriptor_for_document_resource(
            self, raw_resource: typing.Dict) -> resourcedescriptor.RecordDescription:
        raise NotImplementedError
