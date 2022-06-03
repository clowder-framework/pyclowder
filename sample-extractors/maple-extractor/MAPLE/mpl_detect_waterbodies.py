# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 14:22:08 2020

@author: skaiser
"""
"""
 * Python script to demonstrate simple adaptive thresholding with Otsu.
 *
 * usage: python script.py <path_in> <order-shape> <sitename>
"""
#### Imports

import os, sys, fnmatch, json
import numpy as np
import skimage.color
import skimage.filters
import skimage.io
import skimage.viewer
from osgeo import gdal, ogr
import datetime, time
from skimage.morphology import disk
import time
from mpl_config import  MPL_Config

# %%
start = datetime.datetime.now()
start_time = time.time()

# %%
#### Functions

def createFolder(directory):  # from https://gist.github.com/keithweaver/562d3caa8650eefe7f84fa074e9ca949
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)


def detect_waterbodies(input_image_path,path_out):
    # %%
    start = time.time()
    worker_root = MPL_Config.WORKER_ROOT
    #path_out = os.path.join(worker_root, "water_shp")

    nir_band = 3  # set number of NIR band

    time1 = time.time()
    print(time1 - start)
    file_name = input_image_path.rsplit('/')[-1]
    # cmd = "gdal_translate -ot Byte -of GTiff %s %s/%s_8B.tif" %(i, path_out, i[:-4])
    #cmd = "gdal_translate -ot Byte -of GTiff %s %s/%s_8B.tif" % (input_image_path,path_out,file_name)
    #print(cmd)
    #os.system(cmd)

    maksed_file = "%s/%s_8B.tif" % (path_out, file_name)
    kwargs = {
        'format' : 'GTiff',
        'outputType' : gdal.GDT_Byte
    }
    gdal.Translate(maksed_file,input_image_path, **kwargs)
    print("done translate")


    # %% Median and Otsu
    value = 5
    clips = []

    print(maksed_file)

    time1 = time.time()
    print(time1 - start)


    image = skimage.io.imread(maksed_file)  # image[rows, columns, dimensions]-> image[:,:,3] is near Infrared
    nir = image[:, :, nir_band]
    bilat_img = skimage.filters.rank.median(nir, disk(value))

    gtif = gdal.Open(maksed_file)
    geotransform = gtif.GetGeoTransform()
    sourceSR = gtif.GetProjection()

    x = np.shape(image)[1]
    y = np.shape(image)[0]
    bands = np.shape(image)[2]

    # blur and grayscale before thresholding
    blur = skimage.color.rgb2gray(bilat_img)
    blur = skimage.filters.gaussian(blur, sigma=2.0)

    t = skimage.filters.threshold_otsu(blur)
    print(t)

    # perform inverse binary thresholding
    mask = blur < t
    print(mask.dtype)

    # output np array as GeoTiff
    file_out = '%s_t%s_median%s_otsu.tif' % (maksed_file[:-4], str(t)[0:4], str(value))
    clips.append(file_out[:-4] + '.shp')
    dst_ds = gdal.GetDriverByName('GTiff').Create(file_out, x, y, 1, gdal.GDT_Byte)
    dst_ds.GetRasterBand(1).WriteArray(mask)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(sourceSR)
    dst_ds.FlushCache()
    dst_ds = None
    print(file_out)
    #return

    # polygonize and write to Shapefile
    #cmd = 'gdal_polygonize.py %s -f "ESRI Shapefile" %s/%s.shp' % (file_out, path_out,file_out[:-4])
    #os.system(cmd)
    #print(cmd)

    drv = ogr.GetDriverByName("ESRI Shapefile")
    dst_ds = drv.CreateDataSource('%s.shp' % (file_out[:-4]))
    dts_layar = dst_ds.CreateLayer("WaterBodies",srs=None)
    src_ds = gdal.Open(file_out)
    src_band = src_ds.GetRasterBand(1)
    gdal.Polygonize(src_band,None,dts_layar,-1,[],callback=None)

    time1 = time.time()
    print(time1 - start)

    now = datetime.datetime.now()
    print('started at %s,  finished at %s' % (str(start), str(now)))
    print('Total computing time: --------------- %s seconds -----------------' % (time.time() - start_time))
