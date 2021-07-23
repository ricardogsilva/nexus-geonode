from django.shortcuts import render
from django.views.generic import ListView

from pdn.models import *

class NewsPageView(ListView):
    model = News

class ProjectsPageView(ListView):
    model = Project

class AlertsPageView(ListView):
    model = Alert

class ExpertsPageView(ListView):
    model = Expert

