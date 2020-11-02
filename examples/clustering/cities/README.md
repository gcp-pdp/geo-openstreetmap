# List of Cities

In the `cities.csv` there is a list of cities from the [Globalization and World Cities Research Network](https://en.wikipedia.org/wiki/Globalization_and_World_Cities_Research_Network) wiki page.
Each city row contains lat/long and radius of the manually defined circle that approximately covers city infrastructure and agglomeration.

The `query.py` script can be used to transform CSV into the SQL with cities data.
In our example we are saving result of the query into the `osm_cities.cities` table using the BigQuery console.
