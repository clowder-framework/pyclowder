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

import json



# ---------------------- Preset parameter ----------------------
# input path
root_dir = MPL_Config.WORKER_ROOT
shpfile_name = "Prudhoe.shp"

#shpfile_name = "selection_50_51.shp"
shape_file = os.path.join(root_dir,"footprint/Elios/%s"%shpfile_name)

#shape_file_out = os.path.join(root_dir, "shapefiles/Footprints/newShapefile.shp")
#input_image_path = os.path.join(image_root,"WV02_20100729000202_pansh.tif")
# ---------------------- crop image ----------------------
# Open datasets
print(shape_file)
RasterFormat = 'GTiff'
VectorFormat = 'ESRI Shapefile'
poly_shp_file = shapefile.Reader(shape_file)
shapes_poly = poly_shp_file.shapes()
records_poly = poly_shp_file.records()

d = defaultdict(set)
image_dict = {}

len_shape = len(shapes_poly)
finished = np.zeros(len_shape)

f_wv02 = open("data/footprint/Elios/WV02_filenames_%s_.txt"%shpfile_name,'w')


for id_1 in range(len_shape):
    r = records_poly[id_1]
    image_name = records_poly[id_1][1] + "_u16rf3413_pansh.tif"
    #im_name = image_name.split('.tif')[0]
    sensor = records_poly[id_1][3]
    f_wv02.write("%s\n"%image_name)
