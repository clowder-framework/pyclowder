#!/usr/bin/env python3

""" Run this script on local machine rather than AWS ec2
"""
import shapefile
import os.path, os
from shapely.geometry import Polygon
from osgeo import ogr
from scipy.spatial import distance
import numpy as np
import random
from collections import defaultdict

import os.path
import shutil
import datetime
import os
from mpl_config import MPL_Config
import csv
import sys

# work tag
WORKTAG = 1
DIETAG = 0

def stitch_shapefile(input_root,output_root,output_file):
    # create a output shapefile
    print(f"############# {os.getenv('CONDA_PREFIX')}")
    output_path_1 = os.path.join(output_root, "%s.shp" % output_file)
    print("*************************")
    print(output_path_1)

#    final_name = "final_shapefile"
#    output_path_1 = os.path.join(output_root, "%s.shp" % final_name)
    print("*************************")
 #   print(output_path_1)
    w = shapefile.Writer(output_path_1)

    shape_size = 80000

    import time, datetime

    time_file_name = "asprs_shapefile_time.csv"
    time_file_path = os.path.join('data/footprint/shapes', time_file_name)
    time_file = open(time_file_path, "a+")
    csv_w = csv.writer(time_file,)
    t0 = datetime.datetime.now()

    count = 1
    for f in os.listdir(input_root):
         if f.endswith("shp"):
             shp_file_path = os.path.join(input_root,f)
             r = shapefile.Reader(shp_file_path)
             w.fields = r.fields[1:]  # skip first deletion field

             for shaperec in r.iterShapeRecords():
                 w.record(*shaperec.record)
                 w.shape(shaperec.shape)
                 count += 1

    shape_size = count/4


    #         ## w._shapes.extend(r.shapes())
    #         ## w.records.extend(r.records())

    #         ## print ("Num. shapes: %s" % len(w._shapes))
    #
    # # create attribute field as same as input
    #w.fields = list(r.fields)


    # save shp file
    # output_path_1 = os.path.join(output_root,"%s.shp"%final_name)
    w.close()

    

    # read the shape file with recording the index
    sf = shapefile.Reader(output_path_1)
    plyn_shp = sf.shapes()  


    # create a list to store those centroid point
    centroid_list = list()


    # create a count number for final checking
    count = 0
    for current_plyn_id in range(len(plyn_shp)):
        
        current_plyn_vtices = plyn_shp[current_plyn_id].points
        
        # create a polygon in shapely
        ref_polygon = Polygon(current_plyn_vtices)
        
        # parse wkt return
        geom = ogr.CreateGeometryFromWkt(ref_polygon.centroid.wkt)
        centroid_x, centroid_y = geom.GetPoint(0)[0],geom.GetPoint(0)[1]
        
        centroid_list.append([centroid_x, centroid_y])


    # process block by block in case of running out of RAM 
    block_size = 1000
    # set the threshold for filtering out 
    threshold = 3


    close_list = list()

    print ("Total number of polygons: ", len(centroid_list))

    for row in range(0,len(centroid_list),block_size):
        for col in range(0,len(centroid_list),block_size):
            start_r = row
            end_r = min(start_r+block_size,len(centroid_list))
            start_c = col
            end_c = min(start_c+block_size,len(centroid_list))
            
            # get the computing controid list
            row_centroid_list = centroid_list[start_r:end_r]
            col_centroid_list = centroid_list[start_c:end_c]
            
            # calculate their Euclidean distance
            dst_array = distance.cdist(row_centroid_list,col_centroid_list,'euclidean') 

            # filter out close objects
            filter_object_array = np.argwhere((dst_array<30) & (dst_array!=0))
            filter_object_array[:,0] = filter_object_array[:,0] + start_r
            filter_object_array[:,1] = filter_object_array[:,1] + start_c
            
            if filter_object_array.shape[0] != 0:
                for i in filter_object_array:
                    close_list.append(i.tolist())
            else:
                continue
                
    # remove duplicated index
    close_list =  set(frozenset(sublist) for sublist in close_list)
    close_list = [list(x) for x in close_list]



    # --------------- looking for connected components in a graph ---------------  
    def connected_components(lists):
        neighbors = defaultdict(set)
        seen = set()
        for each in lists:
            for item in each:
                neighbors[item].update(each)
        def component(node, neighbors=neighbors, seen=seen, see=seen.add):
            nodes = set([node])
            next_node = nodes.pop
            while nodes:
                node = next_node()
                see(node)
                nodes |= neighbors[node] - seen
                yield node
        for node in neighbors:
            if node not in seen:
                yield sorted(component(node))
                   
    close_list = list(connected_components(close_list))



    # --------------- create a new shp file to store --------------- 
    # randomly pick one of many duplications
    del_index_list = list()
    for close_possible in close_list:     
        random_id = random.choice(close_possible)
        del_index_list.append(random_id)
    del_index_list = sorted(del_index_list)


    # open the target shapefile
    ds = ogr.Open(output_path_1, True)  # True allows to edit the shapefile
    lyr = ds.GetLayer()

    # delete object based on the list
    print("Features before: {}".format(lyr.GetFeatureCount()))
    offset_value = 0
    for del_index in del_index_list:
        lyr.DeleteFeature(del_index)
        offset_value += 1


    # Repack and recompute extent
    # This is not mandatory but it organizes the FID's (so they start at 0 again and not 1)
    # and recalculates the spatial extent.
    ds.ExecuteSQL('REPACK ' + lyr.GetName())
    ds.ExecuteSQL('RECOMPUTE EXTENT ON ' + lyr.GetName())

    print("Features after: {}".format(lyr.GetFeatureCount()))

    del ds

    t1 = datetime.datetime.now()

    delta1 = t1 - t0
    tt1 = (delta1.total_seconds())
    #time_file.writelines("%d, %f\n" % (4*shape_size,tt1))
    csv_w.writerow([4*shape_size,tt1])
    time_file.close()

    print ("Finished")



def main(input_img_name):

    sys.path.append(MPL_Config.ROOT_DIR)

    crop_size = MPL_Config.CROP_SIZE

    # worker roots




    # Create subfolder for each image
    new_file_name = input_img_name.split('.tif')[0]

    worker_input_shp_subroot = os.path.join('data/footprint/shapes/input_shp', new_file_name)
    worker_output_shp_subroot = os.path.join('data/footprint/shapes/final_shp', new_file_name)

    try:
        shutil.rmtree(worker_output_shp_subroot)
    except:
        print("director deletion failed")
        pass
    os.mkdir(worker_output_shp_subroot)



    stitch_shapefile(worker_input_shp_subroot,
                            worker_output_shp_subroot, new_file_name)

    return "done Divide"



if __name__ == '__main__':



    main('WV02_20140612211948_103001003375AC00_14JUN12211948-M1BS-013515292010_01_P009_u16rf3413_pansh.tif')
