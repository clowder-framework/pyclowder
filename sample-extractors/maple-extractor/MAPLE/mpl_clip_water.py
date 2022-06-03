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


org_tif = r"data/input_img/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh.tif"
out_tif_nd =r"data/water_mask/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh/temp_nodata.tif"
out_tif_12 =r"data/water_mask/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh/temp_12.tif"
out_tif_final =r"data/water_mask/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh/temp_final.tif"
in_water =r"data/water_mask/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh_watermask.tif"
out_tif_nd_filled =r"data/water_mask/WV02_20100724230825_103001000638E300_10JUL24230825-M1BS-500336668060_01_P005_u16rf3413_pansh/temp_nodata_filled.tif"

cmd = 'gdal_calc.py -A %s --outfile %s  --calc="(A+1)"'%(in_water,out_tif_12)
os.system(cmd)

cmd = 'gdal_calc.py -A %s --outfile %s --NoDataValue 0 --calc="1*(A>0)"'%(org_tif,out_tif_nd)
os.system(cmd)


cmd = 'gdal_calc.py -A %s  -B %s --outfile %s  --calc="A*(B>0)"'%(out_tif_12, out_tif_nd, out_tif_final)
os.system(cmd)