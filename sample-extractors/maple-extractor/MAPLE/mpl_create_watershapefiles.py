import shapefile
import os.path, os
from shapely.geometry import Polygon
from osgeo import ogr
from scipy.spatial import distance
import numpy as np
import random
from collections import defaultdict
import pickle
from mpl_config import MPL_Config
import shutil
#gdalwarp -dstnodata "16" -cutline WV03_20160724225148_104001001F2E7B00.shp WV03_20160724225148_104001001F2E7B00_16JUL24225148-M1BS-500849241050_01_P001_u16rf3413_pansh_watermask.tif WV03_20160724225148_104001001F2E7B00.tif



import glob

shape_file = os.path.join(MPL_Config.OVERLAP_SHAPE_DIR,MPL_Config.OVL_SHAPEFILE)
shape_dir = os.path.join(MPL_Config.WATER_MASK_DIR,"shape_files")

try:
    shutil.rmtree(shape_dir)
except:
    print("director deletion failed")
    pass
os.mkdir(shape_dir)

RasterFormat = 'GTiff'
VectorFormat = 'ESRI Shapefile'
poly_shp_file = shapefile.Reader(shape_file)
shapes_poly = poly_shp_file.shapes()
records_poly = poly_shp_file.records()
len_shape = len(shapes_poly)



r = shapefile.Reader(shape_file)


for shaperec in r.iterShapeRecords():
    scene_id = shaperec.record['scene_id']
    shape_file = os.path.join(shape_dir,"%s_u16rf3413_pansh.shp"%scene_id)

    w = shapefile.Writer(shape_file)
    w.fields = r.fields[1:]  # skip first deletion field
    w.record(*shaperec.record)
    w.shape(shaperec.shape)

    w.close()


    #p.join()