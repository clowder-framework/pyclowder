import os
from osgeo import osr, gdal,ogr
import shapefile as shp
import numpy as np
import h5py
from mpl_config import  MPL_Config
import cv2
import pickle
from shapely.geometry import Polygon, Point
def divide_image(input_image_path,    # the image directory
                 target_blocksize, file1, file2):   # the crop size

    worker_root = MPL_Config.WORKER_ROOT
    water_dir = MPL_Config.WATER_MASK_DIR

    #get the image name from path
    new_file_name_f = (input_image_path.split('/')[-1])

    new_file_name = new_file_name_f.split('.')[0]

    water_mask_dir = os.path.join(water_dir,new_file_name)
    water_mask = os.path.join(water_mask_dir, "%s_watermask.tif"%new_file_name)
    MASK = gdal.Open(water_mask)
    mask_band = MASK.GetRasterBand(1)
    mask_arry = mask_band.ReadAsArray()

    print(input_image_path)
    IMG1 = gdal.Open(input_image_path)
    gt1 = IMG1.GetGeoTransform()

    ulx, x_resolution, _, uly, _, y_resolution = gt1

    YSize = IMG1.RasterYSize
    XSize = IMG1.RasterXSize

    brx = ulx + x_resolution*XSize
    bry = uly + y_resolution*YSize

    # ---------------------- crop image ----------------------
    #img_band1 = IMG1.GetRasterBand(1)
    img_band2 = IMG1.GetRasterBand(2)
    img_band3 = IMG1.GetRasterBand(3)
    img_band4 = IMG1.GetRasterBand(4)

    #img_array1 = img_band1.ReadAsArray()
    final_array_2 = img_band2.ReadAsArray()
    final_array_3 = img_band3.ReadAsArray()
    final_array_4 = img_band4.ReadAsArray()

    final_array_2 = np.multiply(final_array_2, mask_arry)
    final_array_3 = np.multiply(final_array_3, mask_arry)
    final_array_4 = np.multiply(final_array_4, mask_arry)

    f1 = h5py.File(file1, "w")
    f2 = h5py.File(file2, "w")

    rows_input = IMG1.RasterYSize
    cols_input = IMG1.RasterXSize
    bands_input = IMG1.RasterCount

    # create empty grid cell
    transform = IMG1.GetGeoTransform()

    # ulx, uly is the upper left corner
    ulx, x_resolution, _, uly, _, y_resolution  = transform

    # ---------------------- Divide image ----------------------
    overlap_rate = 0.2
    block_size = target_blocksize
    ysize = rows_input
    xsize = cols_input

    image_count = 0
    #################################################################
    ###
    ##################################################################
    #Load the data frame


    root_dir = MPL_Config.WORKER_ROOT
    shpfile_name = "section_26_27_38_39_50_51_63.shp"

    db_file_path = os.path.join(root_dir, "overlaps/data_frame%s.pkl" % shpfile_name)
    dbfile = open(db_file_path, 'rb')
    ol_dict = pickle.load(dbfile)
    dbfile.close()

    shape_file = os.path.join(root_dir, "overlaps/shapefiles/overlaps/%s" % shpfile_name)
    poly_shp_file = shp.Reader(shape_file)
    shapes_poly = poly_shp_file.shapes()
    records_poly = poly_shp_file.records()

    # db_file_path = os.path.join(MPL_Config.WORKER_ROOT, "footprint/SyntheticOverlaps/image_dict.pkl")
    db_file_path = os.path.join(root_dir, "overlaps/data/image_dict%s.pkl" % shpfile_name)
    dbfile = open(db_file_path, 'rb')
    im_dict = pickle.load(dbfile)
    dbfile.close()


    #find the index in the ol_dict
    poly_id = im_dict[new_file_name_f]
    poly0_vtx = shapes_poly[poly_id].points
    image0_name = records_poly[poly_id][0]


    #f0_name = os.path.join(MPL_Config.OUTPUT_IMAGE_DIR, "txt_%s.txt"%image0_name)
    #f = open(f0_name, 'w')
    #f.write(image0_name)

    poly0 = Polygon(poly0_vtx)
    readF = False

    for id in ol_dict[poly_id]:

        if id == poly_id:
            continue;
        #im_name = records_poly[id][0]
        #f_name = os.path.join(MPL_Config.OUTPUT_IMAGE_DIR,  "txt_%s.txt"%im_name)
        #try:
        #    f = open(f_name,'r')
        #except:
        #    readF = True
        #    pass

        #if(readF):
        #    continue

        poly = shapes_poly[id]

        poly1_vtx = shapes_poly[id].points
        sumx = 0
        sumy = 0
        for pt in poly1_vtx[0:-1]:
            sumx += pt[0]
            sumy += pt[1]

        avg_sumx = sumx/(len(poly1_vtx)-1)
        avg_sumy = sumy/(len(poly1_vtx) -1)

        scaled_vtx = [(0.95*(pt[0] - avg_sumx ) + avg_sumx, 0.95 * (pt[1] - avg_sumy) + avg_sumy) for pt in poly1_vtx]

        poly1 = Polygon(scaled_vtx)
        poly0 = poly0.difference(poly1)

    if(poly0.is_empty):
        return
    # ---------------------- Find each Upper left (x,y) for each images ----------------------
    for i in range(0, ysize, int(block_size*(1-overlap_rate))):
        # don't want moving window to be larger than row size of input raster
        if i + block_size < ysize:
            rows = block_size
        else:
            rows = ysize - i

        # read col
        for j in range(0, xsize, int(block_size*(1-overlap_rate))):

            if j + block_size < xsize:
                    cols = block_size
            else:
                cols = xsize - j

            # Upper left (x,y) for each images
            ul_row_divided_img = uly + i * y_resolution
            ul_col_divided_img = ulx + j * x_resolution

            tile_size = MPL_Config.CROP_SIZE/3
            pt_ul = Point(ul_col_divided_img+x_resolution*tile_size, ul_row_divided_img+y_resolution*tile_size)
            not_processed = pt_ul.within(poly0)

            pt_ul1 = Point(ul_col_divided_img - x_resolution * tile_size, ul_row_divided_img - y_resolution * tile_size)
            not_processed1 = pt_ul1.within(poly0)

            if((not_processed is False) and (not_processed1 is False)):
                continue

            #print(f" j={j} i={i} col={cols} row={rows}")
            # get block out of the whole raster
            #todo check the array values is similar as ReadAsArray()
            band_1_array = final_array_4[i:i+rows, j:j+cols]
            band_2_array = final_array_2[i:i+rows, j:j+cols]
            band_3_array = final_array_3[i:i+rows, j:j+cols]

            #print(band_3_array.shape)
            # filter out black image
            if band_3_array[0,0] == 0 and band_3_array[0,-1] == 0 and  \
               band_3_array[-1,0] == 0 and band_3_array[-1,-1] == 0:
                continue

            # stack three bands into one array
            img = np.stack((band_1_array, band_2_array, band_3_array), axis=2)
            cv2.normalize(img, img, 0, 255, cv2.NORM_MINMAX)
            img = img.astype(np.uint8)
            B, G, R = cv2.split(img)
            out_B = cv2.equalizeHist(B)
            out_R = cv2.equalizeHist(R)
            out_G = cv2.equalizeHist(G)
            final_image = cv2.merge((out_B, out_G, out_R))



            data_c = np.array([i,j,ul_row_divided_img,ul_col_divided_img])
            image_count += 1

            f1.create_dataset(f"image_{image_count}", data=final_image)
            f2.create_dataset(f"param_{image_count}", data=data_c)


    values = np.array([x_resolution, y_resolution, image_count])
    f2.create_dataset("values",data=values)



    f1.close()
    f2.close()


