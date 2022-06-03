#!/usr/bin/env python3

""" Run this script on local machine rather than AWS ec2
"""
import shapefile
import os.path, os
import shutil
from mpl_config import MPL_Config
from shapely.geometry import Polygon, Point
from osgeo import ogr
from scipy.spatial import distance
import numpy as np
import random
from collections import defaultdict
# input path
#root_dir = "/media/outputs/Alaska/data_167_168"
root_dir = "data"
input_dir = os.path.join(root_dir,'final_shp')
output_dir = os.path.join(root_dir,'temp_shp')
final_dir = os.path.join(root_dir,'projected_shp')



for dir in os.listdir(input_dir):
    shp_dir = os.path.join(input_dir,dir)
    projected_dir = os.path.join(final_dir,dir)
    temp_dir = os.path.join(output_dir, dir)



    shape_file = os.path.join(shp_dir,"%s.shp"%dir)
    output_shape_file = os.path.join(temp_dir,"%s.shp"%dir)
    projected_shape_file = os.path.join(projected_dir,"%s.shp"%dir)
    print(shape_file)



    try:
        shutil.rmtree(temp_dir)
    except:
        print("director deletion failed")
        pass
    os.mkdir(temp_dir)

    w = shapefile.Writer(output_shape_file)

    try:
        r = shapefile.Reader(shape_file)
    except:
        continue
    #w.fields = r.fields[1:]  # skip first deletion field


    w.fields = r.fields[1:]  # skip first deletion field

    w.field("Sensor","C","10")
    w.field("Date","C","10")
    w.field("Time", "C", "10")
    w.field("CatalogID", "C", "20")
    w.field("Area", "N", decimal=3)
    w.field("CentroidX", "N", decimal=3)
    w.field("CentroidY", "N", decimal=3)
    w.field("Perimeter", "N", decimal=3)
    w.field("Length", "N", decimal=3)
    w.field("Width", "N", decimal=3)

    for shaperec in r.iterShapeRecords():
        rec  = shaperec.record
        rec.append(dir[0:4])
        rec.append(dir[5:13])
        rec.append(dir[13:19])
        rec.append(dir[20:36])



        poly_vtx = shaperec.shape.points
        poly = Polygon(poly_vtx)
        area = poly.area
        perimeter =poly.length
        box = poly.minimum_rotated_rectangle
        x,y = box.exterior.coords.xy
        centroid = poly.centroid

        p0 = Point(x[0],y[0])
        p1 = Point(x[1], y[1])
        p2 = Point(x[2], y[2])
        edge_lenth = (p0.distance(p1),p1.distance(p2))
        length = max(edge_lenth)
        width = min(edge_lenth)
        rec.append(area)
        rec.append(centroid.x)
        rec.append(centroid.y)
        rec.append(perimeter)
        rec.append(length)
        rec.append(width)
        w.record(*rec)
        w.shape(shaperec.shape)
  #      print(f"{area} : {perimeter} : {length} : {width} ")

    w.close()

    try:
        shutil.rmtree(projected_dir)
    except:
        print("director deletion failed")
        pass
    os.mkdir(projected_dir)

    cmd = "ogr2ogr %s -a_srs 'EPSG:3413' %s"%(projected_shape_file,output_shape_file)
    os.system(cmd)



