from django.db import models


class News(models.Model):
    remote_id = models.BigIntegerField(unique=True)
    source_id = models.BigIntegerField()
    title = models.CharField(max_length=250)
    url = models.CharField(max_length=1000)
    country = models.CharField(max_length=25)
    country_code = models.CharField(max_length=10)
    date = models.DateTimeField()
    source = models.CharField(max_length=100)


class Project(models.Model):
    remote_id = models.BigIntegerField(unique=True)
    name = models.CharField(max_length=250)
    acronym = models.CharField(max_length=250)
    description = models.TextField()
    logo_url = models.CharField(max_length=1000)
    url = models.CharField(max_length=1000)
    active = models.BooleanField(default=False)


class Alert(models.Model):
    remote_id = models.BigIntegerField(unique=True)
    content = models.TextField()
    countries = models.CharField(max_length=1000)
    daterecieved = models.DateTimeField()
    ignore = models.BooleanField(default=True)
    subject = models.TextField()
    uuid = models.CharField(max_length=100)
    source_id = models.BigIntegerField()


class Expert(models.Model):
    remote_id = models.BigIntegerField(unique=True)
    name = models.CharField(max_length=250)
    title = models.CharField(max_length=250)
    country = models.CharField(max_length=25)
    country_code = models.CharField(max_length=10)
    email = models.CharField(max_length=250)
    ministry = models.CharField(max_length=250)
    country_id = models.CharField(max_length=100)
