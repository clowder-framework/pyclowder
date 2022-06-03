import os
from osgeo import osr, gdal,ogr
import shapefile as shp
import numpy as np
import h5py
from mpl_config import  MPL_Config
import cv2
from shapely.geometry import  shape
import fiona

input_shp_iwp = 'data/shp/iwp/WV02_20190811225726_103001009A715900_19AUG11225726-M1BS-503878123100_01_P002_u16rf3413_pansh.shp'
input_shp_select = 'data/shp/ouput_3.shp'
with fiona.open(input_shp_iwp) as input:
    meta = input.meta
    with fiona.open(input_shp_select) as select:
        for polygon in select:
            id = polygon['properties']['id']
            ouput_shp_file = 'data/shp/selected_iwp_%s.shp'%id
            with fiona.open(ouput_shp_file, 'w',**meta) as output:
                for feature in input:
                    if shape(feature['geometry']).intersection(shape(polygon['geometry'])):
                        print('2')
                        output.write(feature)
                    #else:
                    #    print(feature['geometry'])
                    #    print(polygon['geometry'])


quit()



input_shape_iwp = 'data/final_shp/GE01_20120820215521_1050410003FF2E00_12AUG20215521-M1BS-501474456100_01_P001_u16rf3413_pansh/GE01_20120820215521_1050410003FF2E00_12AUG20215521-M1BS-501474456100_01_P001_u16rf3413_pansh.shp'
input_shape_select =''
import fiona
from shapely.geometry import shape
from copy import deepcopy

with fiona.open(input_shape_iwp, "r") as iwp:

    with fiona.open(input_shape_select, "r") as selection:

        # create a schema for the attributes
        outSchema =  deepcopy(iwp.schema)
        #outSchema['properties'].update(s.schema['properties'])

        with fiona.open ("TEST7.shp", "w", s.driver, outSchema, s.crs) as output:

            for school in s:
                for neighborhood in n:
                    # check if point is in polygon and set attribute
                    if shape(school['geometry']).within(shape(neighborhood['geometry'])):
                        #school['properties'] = neighborhood['properties']
                    # write out
                        output.write({
                            'properties': neighborhood['properties'],
                            'geometry': neighborhood['geometry']
                        })