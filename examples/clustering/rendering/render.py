import re
import pandas as pd
from staticmap import StaticMap, Polygon

if __name__ == '__main__':
    project_id = 'gcp-pdp-osm-dev'

    df = pd.io.gbq.read_gbq(f'''
          SELECT 
            geohash,
            geog,
            color
          FROM `gcp-pdp-osm-dev.geohash.level_7_results`
          WHERE geohash LIKE "u3300%"
        ''', project_id=project_id)

    m = StaticMap(1024, 768)
    for index, row in df.iterrows():
        match = re.search(r'POLYGON\(\((.*)\)\)', row['geog'])
        path = [[float(p[0]), float(p[1])] for p in [p.split(' ') for p in match.group(1).split(', ')]]
        color = row['color']

        polygon = Polygon(
            path, color + '80', '#00000000', False
        )
        m.add_polygon(polygon)

    image = m.render()
    image.save('map.png')
