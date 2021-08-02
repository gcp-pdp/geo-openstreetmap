import geohash

alphabet = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')


def _adjust_bbox(bbox):
    if bbox['n'] == 90:
        bbox['n'] = 85
    if bbox['s'] == -90:
        bbox['s'] = -85

    return bbox


if __name__ == '__main__':
    rows = []
    for i, g in enumerate(alphabet):
        centroid = geohash.decode(g)
        bbox = _adjust_bbox(geohash.bbox(g))
        polygon_wkt = 'Polygon(({e} {s}, {e} {n}, {w} {n}, {w} {s}, {e} {s}))'.format(**bbox)
        centroid_wkt = 'POINT ({1} {0})'.format(*centroid)
        rows.append(
            'SELECT "{0}" as geohash,'
            ' "{1}" as geog_wkt,'
            ' ST_GEOGFROMTEXT("{1}", planar => TRUE) as geog,'
            ' ST_GEOGFROMTEXT("{2}") as centroid,'
            ' {3} as geohash_l1_decimal'
            .format(g, polygon_wkt, centroid_wkt, i)
        )

    print('\nUNION ALL '.join(rows))
