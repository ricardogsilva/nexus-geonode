from django.contrib import admin
from . import models
from .models import Alert, Expert, News, Project

@admin.register(models.Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "remote_id",
        "title",
        "summary",
        "link",
        "gdacs_eventtype",
        "gdacs_eventid",
        "gdacs_country",
    )


@admin.register(models.Expert)
class ExpertAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "remote_id",
        "name",
        "email",
        "title",
        "country",
        "ministry",
    )


@admin.register(models.News)
class NewsAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "remote_id",
        "title",
        "url",
        "date",
    )


@admin.register(models.Project)
class ProjectAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "remote_id",
        "name",
        "url",
    )

