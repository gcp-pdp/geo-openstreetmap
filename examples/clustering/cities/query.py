import csv

FIRST_CITY_SQL = 'SELECT "{}" as city_name, "{}" as city_class, {} as latitude, {} as longitude, {} as radius'
CITY_SQL = 'SELECT "{}", "{}", {}, {}, {}'

QUERY = """
WITH cities AS ({})
SELECT
  city_name,
  city_class,
  ST_GEOGPOINT(longitude, latitude) as center,
  radius
FROM cities
"""

if __name__ == '__main__':
    with open('cities.csv', newline='') as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader]
        first_city = rows[1]
        cities_tail = rows[2:]

        cities_sql = ' UNION ALL\n'.join([FIRST_CITY_SQL.format(*first_city)] + [CITY_SQL.format(*city) for city in cities_tail])
        print(QUERY.format(cities_sql))
