# nexus-geonode
Nexus Platform

### Specific deployment steps
 - Load Nexus fixtures (beyond the ones for GeoNode)
   - `python manage.py loaddata ./fixtures/nexus_menus`
   - `python manage.py loaddata ./fixtures/nexus_harvesters`
