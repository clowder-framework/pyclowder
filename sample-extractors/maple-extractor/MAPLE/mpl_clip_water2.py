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

dir = "/media/outputs/shp/test1"

org_tif =  os.path.join(dir,"WV02_20100720235617_1030010006016800_10JUL20235617-M1BS-500173387200_01_P001_u16rf3413_pansh.tif")
out_tif_nd =os.path.join(dir,"temp_nodata.tif")
out_tif_12 =os.path.join(dir,"temp_12.tif")
out_tif_final =os.path.join(dir,"temp_final.tif")
in_water =os.path.join(dir,"WV02_20100720235617_1030010006016800_10JUL20235617-M1BS-500173387200_01_P001_u16rf3413_pansh_watermask.tif")
out_tif_nd_filled =os.path.join(dir,"temp_nodata_filled.tif")

cmd = 'gdal_calc.py -A %s --outfile %s  --calc="(A+1)"'%(in_water,out_tif_12)
os.system(cmd)

cmd = 'gdal_calc.py -A %s --outfile %s --NoDataValue 0 --calc="1*(A>0)"'%(org_tif,out_tif_nd)
os.system(cmd)


cmd = 'gdal_calc.py -A %s  -B %s --outfile %s  --calc="A*(B>0)"'%(out_tif_12, out_tif_nd, out_tif_final)
os.system(cmd)