from django.urls import path

from . import views

urlpatterns = [
    path('news/', views.NewsPageView.as_view(), name='pdn_news'),
    path('projects/', views.ProjectsPageView.as_view(), name='pdn_projects'),
    path('alerts/', views.AlertsPageView.as_view(), name='pdn_alerts'),
    path('experts/', views.ExpertsPageView.as_view(), name='pdn_experts'),
]