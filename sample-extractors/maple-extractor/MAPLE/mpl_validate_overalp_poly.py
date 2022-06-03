import pickle
import os
import shapefile as shp
from mpl_config import  MPL_Config
from shapely.geometry import Polygon, Point
root_dir = MPL_Config.WORKER_ROOT

shpfile_name = "selection_226_227.shp"
shape_file = os.path.join(root_dir,"footprint/overlaps8/%s"%shpfile_name)

# Load the data frame
#db_file_path = os.path.join(MPL_Config.WORKER_ROOT, "footprint/SyntheticOverlaps/data_frame.pkl")
db_file_path = os.path.join(root_dir,"footprint/overlaps8/data/data_frame%s.pkl"%shpfile_name)
dbfile = open(db_file_path, 'rb')
ol_dict = pickle.load(dbfile)
dbfile.close()

shape_file = os.path.join(root_dir,"footprint/overlaps8/shapefiles/%s"%shpfile_name)
poly_shp_file = shp.Reader(shape_file)
shapes_poly = poly_shp_file.shapes()
records_poly = poly_shp_file.records()

#db_file_path = os.path.join(MPL_Config.WORKER_ROOT, "footprint/SyntheticOverlaps/image_dict.pkl")
db_file_path = os.path.join(root_dir,"footprint/overlaps8/data/image_dict%s.pkl"%shpfile_name)
dbfile = open(db_file_path, 'rb')
im_dict = pickle.load(dbfile)
dbfile.close()

# find the index in the ol_dict
poly_id = 776


# f0_name = os.path.join(MPL_Config.OUTPUT_IMAGE_DIR, "txt_%s.txt"%image0_name)
# f = open(f0_name, 'w')
# f.write(image0_name)

ct0 = 0

ol_shp_file = os.path.join(root_dir,"footprint/overlaps8/data/overlap_shapefile_0_80.shp")
w_final = shp.Writer(ol_shp_file)
w_final.field('Name', 'C', size=200)
len_shape = len(shapes_poly)
for poly_id in range(len_shape):
    poly0_vtx = shapes_poly[poly_id].points
    image0_name = records_poly[poly_id][0]
    poly0 = Polygon(poly0_vtx)
    area0 = poly0.area
    readF = False
    print(poly0.area)
    for id in ol_dict[poly_id]:

        if id == poly_id:
            continue;


        poly = shapes_poly[id]

        poly1_vtx = shapes_poly[id].points
        sumx = 0
        sumy = 0
        for pt in poly1_vtx[0:-1]:
            sumx += pt[0]
            sumy += pt[1]

        avg_sumx = sumx / (len(poly1_vtx) - 1)
        avg_sumy = sumy / (len(poly1_vtx) - 1)

        scaled_vtx = [(0.8 * (pt[0] - avg_sumx) + avg_sumx, 0.8 * (pt[1] - avg_sumy) + avg_sumy) for pt in poly1_vtx]

        poly1 = Polygon(scaled_vtx)
        INT = poly0.intersection(poly1)
        #if(INT.area/area0 > 0.1):
        poly0 = poly0.difference(poly1)

    #print(poly0)

    if(poly0.is_empty):
        s=1
        print("%s :: empty "%(poly_id))
    else:
        ptc= poly0.centroid
        ct0 = ct0 + 1
        #print("%d :: %s :: not empty "%(ct0,poly_id))
        #     vertices.append([ulx, uly])
        #w_final.record(poly_id)
        #w_final.poly([poly0.boundary.coords])
        if(poly0.type == 'MultiPolygon'):
            for p in poly0:
                w_final.record(poly_id)
                w_final.poly([list(p.exterior.coords)])
                print("%s :: notempty " % (poly_id))
                print(poly0.area)
        else:
            w_final.record(poly_id)
            w_final.poly([list(poly0.exterior.coords)])
            print("%s :: notempty " % (poly_id))
            print(poly0.area)

w_final.close()