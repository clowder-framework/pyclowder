import os
from osgeo import osr, gdal,ogr
import shapefile as shp
import numpy as np
import h5py
from mpl_config import  MPL_Config
import cv2


def divide_image(input_image_path,    # the image directory
                 target_blocksize, file1, file2):   # the crop size

    worker_root = MPL_Config.WORKER_ROOT
    water_dir = MPL_Config.WATER_MASK_DIR

    #get the image name from path
    new_file_name = (input_image_path.split('/')[-1])

    new_file_name = new_file_name.split('.')[0]

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

    #Load the data frame
    from collections import defaultdict
    dict_ij = defaultdict(dict)
    dict_n = defaultdict(dict)
    tile_count = 0

    y_list = range(0, ysize, int(block_size*(1-overlap_rate)))
    x_list = range(0, xsize, int(block_size*(1-overlap_rate)))
    dict_n['total'] = [len(y_list),len(x_list)]
    # ---------------------- Find each Upper left (x,y) for each images ----------------------
    for id_i,i in enumerate(y_list):

        # don't want moving window to be larger than row size of input raster
        if i + block_size < ysize:
            rows = block_size
        else:
            rows = ysize - i

        # read col
        for id_j, j in enumerate(x_list):

            if j + block_size < xsize:
                    cols = block_size
            else:
                cols = xsize - j
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

            dict_ij[id_i][id_j] = tile_count
            dict_n[tile_count] = [id_i, id_j]

            #print(dict_n[tile_count])


            # stack three bands into one array
            img = np.stack((band_1_array, band_2_array, band_3_array), axis=2)
            cv2.normalize(img, img, 0, 255, cv2.NORM_MINMAX)
            img = img.astype(np.uint8)
            B, G, R = cv2.split(img)
            out_B = cv2.equalizeHist(B)
            out_R = cv2.equalizeHist(R)
            out_G = cv2.equalizeHist(G)
            final_image = cv2.merge((out_B, out_G, out_R))

            # Upper left (x,y) for each images
            ul_row_divided_img = uly + i*y_resolution
            ul_col_divided_img = ulx + j*x_resolution

            data_c = np.array([i,j,ul_row_divided_img,ul_col_divided_img,tile_count])
            image_count += 1

            f1.create_dataset(f"image_{image_count}", data=final_image)
            f2.create_dataset(f"param_{image_count}", data=data_c)
            tile_count += 1
    values = np.array([x_resolution, y_resolution, image_count])
    f2.create_dataset("values",data=values)

    import pickle
    db_file_path = os.path.join(worker_root, "neighbors/%s_ij_dict.pkl" % new_file_name)
    dbfile = open(db_file_path, 'wb')
    pickle.dump(dict_ij, dbfile)
    dbfile.close()

    db_file_path = os.path.join(worker_root, "neighbors/%s_n_dict.pkl" % new_file_name)
    dbfile = open(db_file_path, 'wb')
    pickle.dump(dict_n, dbfile)
    dbfile.close()
    f1.close()
    f2.close()


