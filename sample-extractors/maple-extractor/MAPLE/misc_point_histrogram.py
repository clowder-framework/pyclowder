import shutil
import argparse
import  sys
import os

import matplotlib.pyplot as plt
import shapefile

from mpl_config import MPL_Config
from osgeo import osr, gdal,ogr
import shapefile as shp
import numpy as np
import matplotlib.pyplot as plt


def find_point_desity(input_img_name):

    sys.path.append(MPL_Config.ROOT_DIR)

    crop_size = 200

    # worker roots
    worker_root = MPL_Config.WORKER_ROOT
    worker_img_root = MPL_Config.INPUT_IMAGE_DIR
    worker_divided_img_root = MPL_Config.DIVIDED_IMAGE_DIR

    #input image path
    input_img_path = os.path.join(worker_img_root, input_img_name)

    #
    new_file_name = (input_img_path.split('/')[-1])

    new_file_name = new_file_name.split('.')[0]

    IMG1 = gdal.Open(input_img_path)
    gt1 = IMG1.GetGeoTransform()

    ulx, x_resolution, _, uly, _, y_resolution = gt1

    YSize = IMG1.RasterYSize
    XSize = IMG1.RasterXSize

    brx = ulx + x_resolution * XSize
    bry = uly + y_resolution * YSize



    rows_input = IMG1.RasterYSize
    cols_input = IMG1.RasterXSize
    bands_input = IMG1.RasterCount

    # create empty grid cell
    transform = IMG1.GetGeoTransform()

    # ulx, uly is the upper left corner
    ulx, x_resolution, _, uly, _, y_resolution = transform

    # ---------------------- Divide image ----------------------
    overlap_rate = 0
    block_size = crop_size
    ysize = rows_input/block_size
    xsize = cols_input/block_size



    gridx = np.linspace(ulx,brx,int(xsize))
    gridy = np.linspace(bry,uly,int(ysize))

    root_dir = "data"

    projected_dir = os.path.join(root_dir, 'projected_shp')
    projected_dir = os.path.join(projected_dir, new_file_name)
    polygon_shapefile = os.path.join(projected_dir,"%s.shp"%new_file_name)

    r = shapefile.Reader(polygon_shapefile)

    shapes_poly = r.shapes()
    records_poly = r.records()

    len_shape = len(shapes_poly)
    finished = np.zeros(len_shape)

    pt_x = []
    pt_y = []
    for id_1 in range(len_shape):
        r = records_poly[id_1]

        centroid_x = records_poly[id_1][7]
        centroid_y = records_poly[id_1][8]
        pt_x.append(centroid_x)
        pt_y.append(centroid_y)
    image_count = 0
    # ---------------------- Find each Upper left (x,y) for each images ----------------------

    grid,_,_ = np.histogram2d(pt_x, pt_y, bins=[gridx,gridy])

    plt.figure()
    plt.scatter(pt_x,pt_y,s=1)


    plt.figure()
    plt.pcolormesh(gridy,gridx,grid)
    #plt.scatter(pt_x,pt_y,s=2)
    plt.colorbar()

    #plt.show()
    nCols = len(gridy) - 1
    nRows = len(gridx) - 1

    for ii in range(1,nRows-1):
        for jj in range(1,nCols-1):
            avg = int((grid[ii-1][jj-1] + grid[ii-1][jj] + grid[ii-1][jj+1] + grid[ii][jj-1] +  grid[ii-1][jj+1] +
            grid[ii+1][jj-1] + grid[ii+1][jj]+ grid[ii+1][jj+1])/8)
            diff = (grid[ii][jj]-avg)

            if (int(grid[ii][jj]) != 0 and diff < 0):
                print(diff/grid[ii][jj])


    print("finished tiling")


#############################################################
parser = argparse.ArgumentParser(
    description='Train Mask R-CNN to detect balloons.')

parser.add_argument("--image", required=False,
                    #default='WV02_20110822220744_103001000D6F8F00_11AUG22220744-M1BS-500267988060_01_P005_u16rf3413_pansh.tif',
                    default='test_image_01.tif',
                    metavar="<command>",
                    help="Image name")

args = parser.parse_args()

image_name = args.image
find_point_desity(image_name)