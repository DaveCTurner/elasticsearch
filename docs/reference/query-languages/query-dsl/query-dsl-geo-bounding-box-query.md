---
navigation_title: "Geo-bounding box"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-bounding-box-query.html
---

# Geo-bounding box query [query-dsl-geo-bounding-box-query]


Matches [`geo_point`](/reference/elasticsearch/mapping-reference/geo-point.md) and [`geo_shape`](/reference/elasticsearch/mapping-reference/geo-shape.md) values that intersect a bounding box.


## Example [geo-bounding-box-query-ex]

Assume the following documents are indexed:

```console
PUT /my_locations
{
  "mappings": {
    "properties": {
      "pin": {
        "properties": {
          "location": {
            "type": "geo_point"
          }
        }
      }
    }
  }
}

PUT /my_locations/_doc/1
{
  "pin": {
    "location": {
      "lat": 40.12,
      "lon": -71.34
    }
  }
}

PUT /my_geoshapes
{
  "mappings": {
    "properties": {
      "pin": {
        "properties": {
          "location": {
            "type": "geo_shape"
          }
        }
      }
    }
  }
}

PUT /my_geoshapes/_doc/1
{
  "pin": {
    "location": {
      "type" : "polygon",
      "coordinates" : [[[13.0 ,51.5], [15.0, 51.5], [15.0, 54.0], [13.0, 54.0], [13.0 ,51.5]]]
    }
  }
}
```

Use a `geo_bounding_box` filter to match `geo_point` values that intersect a bounding box. To define the box, provide geopoint values for two opposite corners.

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": {
              "lat": 40.73,
              "lon": -74.1
            },
            "bottom_right": {
              "lat": 40.01,
              "lon": -71.12
            }
          }
        }
      }
    }
  }
}
```

Use the same filter to match `geo_shape` values that intersect the bounding box:

```console
GET my_geoshapes/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": {
              "lat": 40.73,
              "lon": -74.1
            },
            "bottom_right": {
              "lat": 40.01,
              "lon": -71.12
            }
          }
        }
      }
    }
  }
}
```

To match both `geo_point` and `geo_shape` values, search both indices:

```console
GET my_locations,my_geoshapes/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": {
              "lat": 40.73,
              "lon": -74.1
            },
            "bottom_right": {
              "lat": 40.01,
              "lon": -71.12
            }
          }
        }
      }
    }
  }
}
```


## Query options [_query_options]

| Option | Description |
| --- | --- |
| `_name` | Optional name field to identify the filter |
| `validation_method` | Set to `IGNORE_MALFORMED` toaccept geo points with invalid latitude or longitude, set to`COERCE` to also try to infer correct latitude or longitude. (default is `STRICT`). |


## Accepted formats [query-dsl-geo-bounding-box-query-accepted-formats]

In much the same way the `geo_point` type can accept different representations of the geo point, the filter can accept it as well:


### Lat lon as properties [_lat_lon_as_properties_2]

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": {
              "lat": 40.73,
              "lon": -74.1
            },
            "bottom_right": {
              "lat": 40.01,
              "lon": -71.12
            }
          }
        }
      }
    }
  }
}
```


### Lat lon as array [_lat_lon_as_array_2]

Format in `[lon, lat]`, note, the order of lon/lat here in order to conform with [GeoJSON](http://geojson.org/).

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": [ -74.1, 40.73 ],
            "bottom_right": [ -71.12, 40.01 ]
          }
        }
      }
    }
  }
}
```


### Lat lon as string [_lat_lon_as_string]

Format in `lat,lon`.

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": "POINT (-74.1 40.73)",
            "bottom_right": "POINT (-71.12 40.01)"
          }
        }
      }
    }
  }
}
```


### Bounding box as well-known text (WKT) [_bounding_box_as_well_known_text_wkt]

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "wkt": "BBOX (-74.1, -71.12, 40.73, 40.01)"
          }
        }
      }
    }
  }
}
```


### Geohash [_geohash_2]

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top_left": "dr5r9ydj2y73",
            "bottom_right": "drj7teegpus6"
          }
        }
      }
    }
  }
}
```

When geohashes are used to specify the bounding the edges of the bounding box, the geohashes are treated as rectangles. The bounding box is defined in such a way that its top left corresponds to the top left corner of the geohash specified in the `top_left` parameter and its bottom right is defined as the bottom right of the geohash specified in the `bottom_right` parameter.

In order to specify a bounding box that would match entire area of a geohash the geohash can be specified in both `top_left` and `bottom_right` parameters:

```console
GET my_locations/_search
{
  "query": {
    "geo_bounding_box": {
      "pin.location": {
        "top_left": "dr",
        "bottom_right": "dr"
      }
    }
  }
}
```

In this example, the geohash `dr` will produce the bounding box query with the top left corner at `45.0,-78.75` and the bottom right corner at `39.375,-67.5`.


## Vertices [_vertices]

The vertices of the bounding box can either be set by `top_left` and `bottom_right` or by `top_right` and `bottom_left` parameters. Instead of setting the values pairwise, one can use the simple names `top`, `left`, `bottom` and `right` to set the values separately.

```console
GET my_locations/_search
{
  "query": {
    "bool": {
      "must": {
        "match_all": {}
      },
      "filter": {
        "geo_bounding_box": {
          "pin.location": {
            "top": 40.73,
            "left": -74.1,
            "bottom": 40.01,
            "right": -71.12
          }
        }
      }
    }
  }
}
```


## Multi location per document [_multi_location_per_document]

The filter can work with multiple locations / points per document. Once a single location / point matches the filter, the document will be included in the filter


## Ignore unmapped [_ignore_unmapped]

When set to `true` the `ignore_unmapped` option will ignore an unmapped field and will not match any documents for this query. This can be useful when querying multiple indexes which might have different mappings. When set to `false` (the default value) the query will throw an exception if the field is not mapped.


## Notes on precision [_notes_on_precision]

Geopoints have limited precision and are always rounded down during index time. During the query time, upper boundaries of the bounding boxes are rounded down, while lower boundaries are rounded up. As a result, the points along on the lower bounds (bottom and left edges of the bounding box) might not make it into the bounding box due to the rounding error. At the same time points alongside the upper bounds (top and right edges) might be selected by the query even if they are located slightly outside the edge. The rounding error should be less than 4.20e-8 degrees on the latitude and less than 8.39e-8 degrees on the longitude, which translates to less than 1cm error even at the equator.

Geoshapes also have limited precision due to rounding. Geoshape edges along the bounding box’s bottom and left edges may not match a `geo_bounding_box` query. Geoshape edges slightly outside the box’s top and right edges may still match the query.

