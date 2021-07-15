from django.shortcuts import render
from django.views.generic.base import TemplateView

class NewsPageView(TemplateView):

    template_name = "pdn/news.html"


class ProjectsPageView(TemplateView):

    template_name = "pdn/projects.html"


class AlertsPageView(TemplateView):

    template_name = "pdn/alerts.html"


class ExpertsPageView(TemplateView):

    template_name = "pdn/experts.html"

