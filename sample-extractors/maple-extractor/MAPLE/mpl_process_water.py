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

#gdalwarp -dstnodata "16" -cutline WV03_20160724225148_104001001F2E7B00.shp WV03_20160724225148_104001001F2E7B00_16JUL24225148-M1BS-500849241050_01_P001_u16rf3413_pansh_watermask.tif WV03_20160724225148_104001001F2E7B00.tif



import glob

shape_file = r"/media/outputs/water_bodies/site_01/shps/167_168.shp"
input_dir = r"/media/outputs/Alaska/data_167_168/water_bodies/"
out_dir =r"/media/outputs/Alaska/data_178_179_180/water_processed"
zip_dir =r"/media/outputs/Alaska/data_178_179_180/water_processed_zip/"
water_shp = r"/backup/water_shp"

RasterFormat = 'GTiff'
VectorFormat = 'ESRI Shapefile'
poly_shp_file = shapefile.Reader(shape_file)
shapes_poly = poly_shp_file.shapes()
records_poly = poly_shp_file.records()
len_shape = len(shapes_poly)



r = shapefile.Reader(shape_file)


for shaperec in r.iterShapeRecords():
    w = shapefile.Writer("temp.shp")
    w.fields = r.fields[1:]  # skip first deletion field
    w.record(*shaperec.record)
    w.shape(shaperec.shape)

    w.close()

    dir_name  =  "%s_u16rf3413_pansh"%(shaperec.record[1])

    file_dir = os.path.join(input_dir,dir_name)
    file_path = "%s/*.tif"%file_dir
    files = sorted(glob.glob(file_path))
    output_file = "%s/%s_watermask.tif"%(out_dir, shaperec.record[1])
    zip_file = "%s/%s_watermask.tif" % (zip_dir, shaperec.record[1])
    shp_file = "%s/%s_watermask.shp" % (water_shp, shaperec.record[1])

import multiprocessing

def run_process(cmd):
    import os

    print(cmd)
    os.system(cmd)


file_path = "%s/*.tif"%zip_dir
files = sorted(glob.glob(file_path))

end = len(files)
start = 0
step = 4

for st in range(start,end,step):
    selected_files = files[st:st+step]

    for file in selected_files:
            print(file)
            #cmd = "gdalwarp -dstnodata '2' -cutline  temp.shp %s %s" % (file, output_file)
            #os.system(cmd)

            #cmd = "gdal_translate -co compress=LZW %s %s"%(output_file,zip_file)
            #os.system(cmd)

            cmd = 'gdal_polygonize.py %s -f "ESRI Shapefile" %s'%(output_file,shp_file )
            print(cmd)
            #p = multiprocessing.Process(target=run_process, args=(cmd,))
            #p.start()
            #os.system(cmd)

    #p.join()