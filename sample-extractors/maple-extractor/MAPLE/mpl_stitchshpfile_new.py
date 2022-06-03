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


def stitch_shapefile(input_root,output_root,projected_root,output_file,image_name):
    # create a output shapefile

    output_path_1 = os.path.join(output_root, "%s.shp" % output_file)
    output_path_prj = os.path.join(projected_root, "%s.shp" % output_file)
    print(output_path_1)
    #output_path_prj = os.path.join(output_root, "%s_prj_3413.shp" % output_file)
    w = shapefile.Writer(output_path_1)

    import glob
    file_path_shp = os.path.join(input_root,"*.shp")
    files = sorted(glob.glob(file_path_shp))
    file_names = []
    for file in files:
        #file_names.append(file.split('/')[-1])
        print(file)
        r = shapefile.Reader(file)
        w.fields = r.fields[1:]  # skip first deletion field
        for shaperec in r.iterShapeRecords():
             w.record(*shaperec.record)
             w.shape(shaperec.shape)
    w.close()


    import glob
    import pickle
    from collections import defaultdict

    polygon_dict = defaultdict(dict)

    file_path = "data/neighbors/%s_polydict_*.pkl"%image_name
    files = sorted(glob.glob(file_path))
    file_names = []
    poly_count = 0
    for file in files:
        dbfile = open(file, 'rb')
        temp_dict = pickle.load(dbfile)
        for k,v in temp_dict.items():

            polygon_dict[k] = [poly_count,poly_count+v[0]]
            poly_count += v[0]

    #print(polygon_dict)



    from mpl_config import MPL_Config
    worker_root = MPL_Config.WORKER_ROOT
    dict_ij_path = os.path.join(worker_root, "neighbors/%s_ij_dict.pkl" % image_name)
    dbfile = open(dict_ij_path, 'rb')
    dict_ij = pickle.load(dbfile)
    dbfile.close()

    dict_n_path = os.path.join(worker_root, "neighbors/%s_n_dict.pkl" % image_name)
    dbfile = open(dict_n_path, 'rb')
    dict_n = pickle.load(dbfile)
    size_i, size_j = dict_n['total']
    dbfile.close()

    #print(dict_n)

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
    block_size = 100
    # set the threshold for filtering out 
    threshold = 3
    close_list = list()
    print ("Total number of polygons: ", len(centroid_list))
    tile_blocksize = 4

    
    for id_i in range(0, size_i, 3):
        if id_i + tile_blocksize < size_i:
            n_i = tile_blocksize
        else:
            n_i = size_i - id_i

        for id_j in range(0, size_j, 3):
            #print("%d, %d" % (id_i, id_j))
            if id_j + tile_blocksize < size_j:
                n_j = tile_blocksize
            else:
                n_j = size_j - id_j

            # add to the neighbor list.
            centroid_neighbors = []
            poly_neighbors = []
            close_list_local = []

            for ii in range(n_i):
                for jj in range(n_j):
                    #print("(%d %d)"%(ii+id_i,jj+id_j))
                    if (ii+id_i) in dict_ij.keys():
                        if (jj+id_j) in dict_ij[(ii+id_i)].keys():
                           n = dict_ij[ii+id_i][jj+id_j]
                           poly_range = polygon_dict[n]
                           poly_list = [*range(poly_range[0],poly_range[1])]
                           poly_neighbors.extend(poly_list)
                           centroid_neighbors.extend(centroid_list[poly_range[0]:poly_range[1]])
                        #else:
                        #    print("####")
            if(len(centroid_neighbors) == 0):
                continue
            dst_array = distance.cdist(centroid_neighbors, centroid_neighbors, 'euclidean')

            # filter out close objects
            filter_object_array = np.argwhere((dst_array < 10) & (dst_array > 0))

            filter_object_array[:, 0] = [poly_neighbors[i] for i in filter_object_array[:, 0] ]
            filter_object_array[:, 1] =  [poly_neighbors[i] for i in filter_object_array[:, 1] ]

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
        close_possible.remove(random_id)
        del_index_list.extend(close_possible)

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

    #del ds

    cmd = "ogr2ogr %s -a_srs 'EPSG:3413' %s"%(output_path_prj,output_path_1)
    print(cmd)
    os.system(cmd)
    print ("Finished")
