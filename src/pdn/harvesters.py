import copy
import enum
import logging
import typing
import uuid

import dateutil.parser
from geonode.base.models import ResourceBase
from geonode.documents.models import Document
from geonode.harvesting import (
    models as harvesting_models,
    resourcedescriptor
)
from geonode.harvesting.harvesters import base
from nexus.utils import PostgRestClient

from . import models

logger = logging.getLogger(__name__)


class PdnResourceType(enum.Enum):
    ALERT = "alert"
    DOCUMENT = "document"
    EXPERT = "experts"
    NEWS_ARTICLE = "news"
    PROJECT = "project"


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
        if self.remote_url.endswith("/"):
            self.remote_url = self.remote_url[:-1]
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
        return {
            PdnResourceType.DOCUMENT.value: Document
        }[remote_resource_type]

    def get_resource(
            self,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> typing.Optional[resourcedescriptor.RecordDescription]:
        """Harvest a single resource from the remote service"""
        remote_id = harvestable_resource.unique_identifier.rpartition(self._UNIQUE_ID_SEPARATOR)[-1]
        raw_resource = self._api_client.get_resource(f"/{harvestable_resource.remote_resource_type}", remote_id)
        result = None
        if raw_resource is not None:
            if harvestable_resource.remote_resource_type == PdnResourceType.DOCUMENT.value:
                resource_descriptor = self._get_resource_descriptor_for_document_resource(
                    raw_resource,
                    harvestable_resource,
                )
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
            PdnResourceType.DOCUMENT: super().update_geonode_resource,
            PdnResourceType.EXPERT: self._update_expert_record,
            PdnResourceType.NEWS_ARTICLE: self._update_news_record,
            PdnResourceType.PROJECT: self._update_project_record,
        }.get(PdnResourceType(harvestable_resource.remote_resource_type))
        if handler is not None:
            return handler(harvested_info, harvestable_resource, harvesting_session_id)
        else:
            raise RuntimeError(f"Invalid resource type: {harvestable_resource.remote_resource_type}")

    def finalize_resource_update(
            self,
            geonode_resource: ResourceBase,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> ResourceBase:
        if harvestable_resource.remote_resource_type != PdnResourceType.DOCUMENT.value:
            raise RuntimeError(f"Unexpected resource type: {harvestable_resource.remote_resource_type}")
        else:
            geonode_resource.thumbnail_url = harvested_info.resource_descriptor.distribution.thumbnail_url
            geonode_resource.doc_url = harvested_info.resource_descriptor.distribution.original_format_url
            geonode_resource.save()
        return geonode_resource

    def finalize_resource_deletion(self, harvestable_resource: harvesting_models.HarvestableResource):
        record_class = {
            PdnResourceType.ALERT: models.Alert,
            PdnResourceType.EXPERT: models.Expert,
            PdnResourceType.NEWS_ARTICLE: models.News,
            PdnResourceType.PROJECT: models.Project,
        }.get(PdnResourceType(harvestable_resource.remote_resource_type))
        if record_class is not None:
            remote_id = int(harvestable_resource.unique_identifier.rpartition(self._UNIQUE_ID_SEPARATOR)[-1])
            logger.debug(f"remote_id: {remote_id}")
            try:
                record = record_class.objects.get(remote_id=remote_id)
                record.delete()
            except record_class.DoesNotExist:
                logger.exception(
                    f"Could not delete {harvestable_resource.remote_resource_type!r} record with remote "
                    f"id {harvestable_resource.remote_resource_type}"
                )

    def _update_alert_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> None:
        raw_record: typing.Dict = harvested_info.additional_information
        try:
            date_received = dateutil.parser.parse(raw_record["daterecieved"])
        except KeyError:
            date_received = None
        models.Alert.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "content": raw_record.get("content", ""),
                "countries": raw_record.get("countries", ""),
                "daterecieved": date_received,
                "ignore": raw_record.get("ignore", False),
                "subject": raw_record.get("subject", ""),
                "uuid": raw_record.get("uuid", ""),
                "source_id": raw_record.get("source_id", 0),
            }
        )

    def _update_expert_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> None:
        raw_record: typing.Dict = harvested_info.additional_information
        models.Expert.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "name": raw_record.get("name", ""),
                "title": raw_record.get("title", ""),
                "country": raw_record.get("country", ""),
                "country_code": raw_record.get("country_code") or "",
                "email": raw_record.get("email", ""),
                "ministry": raw_record.get("ministry", ""),
                "country_id": raw_record.get("country_id") or "",
            }
        )

    def _update_news_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> None:
        raw_record: typing.Dict = harvested_info.additional_information
        try:
            date_ = dateutil.parser.parse(raw_record["date"])
        except KeyError:
            date_ = None
        models.News.objects.update_or_create(
            remote_id=raw_record["id"],
            defaults={
                "source_id": raw_record.get("source_id", 0),
                "title": raw_record.get("title", ""),
                "url": raw_record.get("url", ""),
                "country": raw_record.get("country", ""),
                "country_code": raw_record.get("country_code") or "",
                "date": date_,
                "source": raw_record.get("source", ""),
            }
        )

    def _update_project_record(
            self,
            harvested_info: base.HarvestedResourceInfo,
            harvestable_resource: harvesting_models.HarvestableResource,
            harvesting_session_id: int
    ) -> None:
        raw_record: typing.Dict = harvested_info.additional_information
        models.Project.objects.update_or_create(
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
                elif resource_type == PdnResourceType.DOCUMENT:
                    title_parts = [
                        record.get("country", ""),
                        record.get("title", ""),
                        record.get("series", ""),
                        record.get("publicationyear", ""),
                    ]
                    title = " - ".join([str(part) for part in title_parts if part != ""])
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
                        title=title[:255],
                        resource_type=resource_type.value,
                    )
                )
        return result

    def _get_resource_descriptor_for_document_resource(
            self,
            raw_resource: typing.Dict,
            harvestable_resource: harvesting_models.HarvestableResource
    ) -> resourcedescriptor.RecordDescription:
        raw_date_stamp = raw_resource.get("uploaddate")
        date_stamp = dateutil.parser.parse(raw_date_stamp) if raw_date_stamp is not None else None
        country = raw_resource.get("country")
        point_of_contact = resourcedescriptor.RecordDescriptionContact(
            role="pointOfContact",
            name=raw_resource.get("authors"),
            organization=raw_resource.get("corporateauthor"),
            position=raw_resource.get("publisher"),
            address_country=country,
        )
        author = copy.deepcopy(point_of_contact)
        author.role = "author"
        download_uri = raw_resource.get("filename")
        if download_uri is not None:
            download_url = f"{self.remote_url}/doc/{download_uri}"
            overview_uri = download_uri.rpartition(".")[0] + ".png"
            graphic_overview_url = f"{self.remote_url}/doc/{overview_uri}"
        else:
            download_url = None
            graphic_overview_url = None
        # NOTE: PDN documents do not have a UUID. As such we generate one when first importing the resource and reuse it when updating it
        if harvestable_resource.geonode_resource is not None:
            uuid_ = uuid.UUID(harvestable_resource.geonode_resource.uuid)
        else:
            uuid_ = uuid.uuid4()
        return resourcedescriptor.RecordDescription(
            uuid=uuid_,
            point_of_contact=point_of_contact,
            author=author,
            date_stamp=date_stamp,
            identification=resourcedescriptor.RecordIdentification(
                name=raw_resource.get("title"),
                title=raw_resource.get("title"),
                date=date_stamp,
                date_type="upload",
                abstract=raw_resource.get("description", ""),
                purpose=raw_resource.get("targetaudicent"),
                originator=author,
                graphic_overview_uri=graphic_overview_url,
                place_keywords=[country] if country is not None else [],
                other_keywords=tuple(),
                license=[],
                supplemental_information=(
                    f"Cataloging source: {raw_resource.get('catalougingsource', '')}\n"
                    f"General Note: {raw_resource.get('generalnote', '')}"
                    f"ISBN: {raw_resource.get('isbn', '')}"
                    f"ISSN: {raw_resource.get('issn', '')}"
                )
            ),
            distribution=resourcedescriptor.RecordDistribution(
                link_url=f"{self.remote_url}/document/{raw_resource['id']}",
                thumbnail_url=graphic_overview_url,
                original_format_url=download_url,
            ),
        )
