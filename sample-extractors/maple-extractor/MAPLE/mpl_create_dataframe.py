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
shpfile_name = "4610_2021aug11.shp"

#shpfile_name = "selection_50_51.shp"
shape_file = os.path.join(root_dir,"footprint/overlaps10/shapefiles/%s"%shpfile_name)

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

f_wv02 = open("data/footprint/overlaps10/data/WV02/WV02_filenames_%s_.txt"%shpfile_name,'w')
f_wv03 = open("data/footprint/overlaps10/data/WV03/WV03_filenames_%s_.txt"%shpfile_name,'w')

for id_1 in range(len_shape):
    r = records_poly[id_1]
    image_name = records_poly[id_1][1] + "_u16rf3413_pansh.tif"
    #im_name = image_name.split('.tif')[0]
    sensor = records_poly[id_1][3]
    if sensor == 'AAAA':
        f_wv02.write("%s\n"%image_name)
    else:
        f_wv03.write("%s\n" % image_name)
    image_dict[image_name] = id_1


    poly1_vtx = shapes_poly[id_1].points
    #im_name  = records_poly[id_1][0]
    polygon1 = Polygon(poly1_vtx)

    #print(f"{id_1} : {len_shape}")
    finished[id_1] = 1
    d[id_1].add(id_1)
    for id_2 in range(len(shapes_poly)):

        if id_1 == id_2:
            continue

        if finished[id_2] == 0:
            continue;

        poly2_vtx = shapes_poly[id_2].points
        polygon2 = Polygon(poly2_vtx)


        try:
            I = polygon1.intersection(polygon2)
            if (I.area > 0):
                d[id_1].add(id_2)
        except:
            print("exp")
            continue
    print(id_1)
    print(d[id_1])
f_wv02.close()
f_wv03.close()
db_file_path = os.path.join(root_dir,"footprint/overlaps9/data/data_frame%s.pkl"%shpfile_name)
dbfile = open(db_file_path,'wb')
pickle.dump(d,dbfile)
dbfile.close()
print(d)

db_file_path = os.path.join(root_dir,"footprint/overlaps9/data/image_dict%s.pkl"%shpfile_name)
dbfile = open(db_file_path,'wb')
pickle.dump(image_dict,dbfile)
print(image_dict)
dbfile.close()


dbfile = open(db_file_path,'rb')
mydict = pickle.load(dbfile)
dbfile.close()



