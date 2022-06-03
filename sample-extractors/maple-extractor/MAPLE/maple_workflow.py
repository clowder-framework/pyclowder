
import shutil
import argparse


import os.path
import shutil
import datetime
import os
from mpl_config import MPL_Config
import mpl_divideimg_234_water_new as divide
import mpl_infer_tiles_GPU_new as inference
import sys
import mpl_stitchshpfile_new as stich
import mpl_process_shapefile as process
import cv2
# work tag
WORKTAG = 1
DIETAG = 0



def tile_image(input_img_name):

    sys.path.append(MPL_Config.ROOT_DIR)

    crop_size = MPL_Config.CROP_SIZE

    # worker roots
    worker_root = MPL_Config.WORKER_ROOT
    worker_img_root = MPL_Config.INPUT_IMAGE_DIR
    worker_divided_img_root = MPL_Config.DIVIDED_IMAGE_DIR

    #input image path
    input_img_path = os.path.join(worker_img_root, input_img_name)

    # Create subfolder for each image
    new_file_name = input_img_name.split('.tif')[0]
    worker_divided_img_subroot = os.path.join(worker_divided_img_root, new_file_name)

    print(worker_divided_img_subroot)

    try:
        shutil.rmtree(worker_divided_img_subroot)
    except:
        print("director deletion failed")
        pass
    os.mkdir(worker_divided_img_subroot)


    file1 = (os.path.join(worker_divided_img_subroot, 'image_data.h5'))
    file2 = (os.path.join(worker_divided_img_subroot, 'image_param.h5'))

    divide.divide_image(input_img_path, crop_size,
                        file1, file2)

    print("finished tiling")

def cal_water_mask(input_img_name):
    from mpl_config import MPL_Config
    import os
    from osgeo import gdal, ogr
    import numpy as np
    import skimage.color
    import skimage.filters
    import skimage.io
    import skimage.viewer
    import shutil
    from skimage.morphology import disk
    import cv2


    image_file_name = (input_img_name).split('.tif')[0]

    worker_root = MPL_Config.WORKER_ROOT
    worker_water_root = MPL_Config.WATER_MASK_DIR #  os.path.join(worker_root, "water_shp")
    temp_water_root =  MPL_Config.TEMP_W_IMG_DIR#os.path.join(worker_root, "temp_8bitmask")

    ouput_image = os.path.join(MPL_Config.OUTPUT_IMAGE_DIR,"%s.tif"%image_file_name)

    worker_water_subroot = os.path.join(worker_water_root, image_file_name)
    temp_water_subroot = os.path.join(temp_water_root, image_file_name)
    worker_water_final_subroot = os.path.join(MPL_Config.WATER_MASK_Final_DIR,image_file_name)
    try:
        shutil.rmtree(worker_water_subroot)
    except:
        pass

    try:
        shutil.rmtree(temp_water_subroot)
    except:
        pass

    try:
        shutil.rmtree(worker_water_final_subroot)
    except:
        pass

        # check local storage for temporary storage
    os.mkdir(worker_water_subroot)
    os.mkdir(temp_water_subroot)
    os.mkdir(worker_water_final_subroot)

    output_watermask = os.path.join(worker_water_subroot, r"%s_watermask.tif" % image_file_name)
    output_tif_8b_file = os.path.join(temp_water_subroot, r"%s_8bit.tif" % image_file_name)
    output_watermask_nodata = os.path.join(temp_water_subroot, r"%s_watermask_nodata.tif" % image_file_name)
    output_watermask_12 = os.path.join(temp_water_subroot, r"%s_watermask_12.tif" % image_file_name)
    output_final_watermask = os.path.join(temp_water_subroot, r"%s_watermask.tif" % image_file_name)
    output_final_watermask_ziped = os.path.join(worker_water_final_subroot, r"%s_watermask.tif" % image_file_name)


    # TODO: THIS WAS 3 -- kastan edit
    nir_band = 3  # set number of NIR band
    # nir_band = 1  # set number of NIR band

    input_image = os.path.join(MPL_Config.INPUT_IMAGE_DIR, input_img_name)

    # %% Median and Otsu
    value = 5
    clips = []

    cmd = "gdal_translate -ot Byte -of GTiff %s %s" % (input_image, output_tif_8b_file)
    os.system(cmd)
    print("reading 8bit file")
    #image = cv2.imread(output_tif_8b_file,cv2.IMREAD_UNCHANGED)
    image = skimage.io.imread(output_tif_8b_file)  # image[rows, columns, dimensions]-> image[:,:,3] is near Infrared
    nir = image[:, :, nir_band]

    bilat_img = skimage.filters.rank.median(nir, disk(value))

    gtif = gdal.Open(input_image)
    geotransform = gtif.GetGeoTransform()
    sourceSR = gtif.GetProjection()

    x = np.shape(image)[1]
    y = np.shape(image)[0]
    bands = np.shape(image)[2]
    print("filtering image")
    # blur and grayscale before thresholding
    blur = skimage.color.rgb2gray(bilat_img)
    print("done color")
    #blur = skimage.filters.gaussian(blur, sigma=2.0)
    blur = cv2.GaussianBlur(blur,(0,0),2,2)
    print("done blur")

    #t = skimage.filters.threshold_otsu(blur)
    #mask = blur > t

    t,mask = cv2.threshold(blur,0,1,cv2.THRESH_BINARY+cv2.THRESH_OTSU)

    # output np array as GeoTiff
    print("saving water maks")
    dst_ds = gdal.GetDriverByName('GTiff').Create(output_watermask, x, y, 1, gdal.GDT_Byte, ['NBITS=1'])
    dst_ds.GetRasterBand(1).WriteArray(mask)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(sourceSR)
    dst_ds.FlushCache()
    dst_ds = None

    #cmd = "gdalwarp -dstnodata '2' -cutline  %s %s %s" % (image_shapefile,output_watermask, output_watermask_nodata)
    #os.system(cmd)

    #cmd = "gdal_translate -co compress=LZW %s %s"%(output_watermask_nodata,output_final_watermask)
    #os.system(cmd)

    return

    cmd = 'gdal_calc.py -A %s --outfile %s  --calc="(A+1)"' % (output_watermask, output_watermask_12)
    os.system(cmd)

    cmd = 'gdal_calc.py -A %s --outfile %s --NoDataValue 0 --calc="1*(A>0)"' % (input_image, output_watermask_nodata)
    os.system(cmd)

    cmd = 'gdal_calc.py -A %s  -B %s --outfile %s  --calc="A*(B>0)"' % (output_watermask_12, output_watermask_nodata, output_final_watermask)
    os.system(cmd)

    cmd = "gdal_translate -co compress=LZW %s %s"%(output_final_watermask,output_final_watermask_ziped)
    os.system(cmd)

    try:
        shutil.rmtree(temp_water_subroot)
    except:
        pass




def infer_image(input_img_name):

    sys.path.append(MPL_Config.ROOT_DIR)

    crop_size = MPL_Config.CROP_SIZE

    # worker roots
    worker_root = MPL_Config.WORKER_ROOT
    worker_img_root = MPL_Config.INPUT_IMAGE_DIR
    worker_divided_img_root = MPL_Config.DIVIDED_IMAGE_DIR

    #input image path
    input_img_path = os.path.join(worker_img_root, input_img_name)

    # Create subfolder for each image
    new_file_name = input_img_name.split('.tif')[0]
    worker_divided_img_subroot = os.path.join(worker_divided_img_root, new_file_name)

    print(worker_divided_img_subroot)


    file1 = (os.path.join(worker_divided_img_subroot, 'image_data.h5'))
    file2 = (os.path.join(worker_divided_img_subroot, 'image_param.h5'))

    worker_output_shp_root = MPL_Config.OUTPUT_SHP_DIR
    worker_output_shp_subroot = os.path.join(worker_output_shp_root, new_file_name)
    try:
        shutil.rmtree(worker_output_shp_subroot)

    except:
        print("director deletion failed")
        pass

    POLYGON_DIR = worker_root
    weights_path = MPL_Config.WEIGHT_PATH

    inference.inference_image(POLYGON_DIR,
                              weights_path,
                              worker_output_shp_subroot, file1, file2,new_file_name)

#    try:
#        shutil.rmtree(worker_divided_img_subroot)

#    except:
        #     print("director deletion failed")
#        pass
    print("done")




def stich_shapefile(input_img_name):

    sys.path.append(MPL_Config.ROOT_DIR)

    crop_size = MPL_Config.CROP_SIZE

    # worker roots
    worker_img_root = MPL_Config.INPUT_IMAGE_DIR

    worker_finaloutput_root =  MPL_Config.FINAL_SHP_DIR
    worker_output_shp_root = MPL_Config.OUTPUT_SHP_DIR
    worker_projected_shp_root = MPL_Config.PROJECTED_SHP_DIR
    # Create subfolder for each image
    new_file_name = input_img_name.split('.tif')[0]

    worker_finaloutput_subroot = os.path.join(worker_finaloutput_root, new_file_name)
    worker_output_shp_subroot = os.path.join(worker_output_shp_root, new_file_name)
    worker_projected_shp_subroot = os.path.join(worker_projected_shp_root, new_file_name)

    try:
        shutil.rmtree(worker_finaloutput_subroot)
    except:
        print("director deletion failed")
        pass
    try:
        shutil.rmtree(worker_projected_shp_subroot)
    except:
        print("director deletion failed")
        pass

    print("MAKE FOLDER HERE", worker_finaloutput_subroot)
    print("MAKE FOLDER HERE", worker_projected_shp_subroot)
    os.mkdir(worker_finaloutput_subroot)
    os.mkdir(worker_projected_shp_subroot)


    stich.stitch_shapefile(worker_output_shp_subroot,
                            worker_finaloutput_subroot, worker_projected_shp_subroot,new_file_name,new_file_name)

    input_prj_file = os.path.join(MPL_Config.WORKER_ROOT, "GCS_WGS_1984.prj")
    output_prj_file = os.path.join(worker_finaloutput_subroot, "%s.prj" % new_file_name)
    shutil.copy(input_prj_file, output_prj_file)
    return "done Divide"

def main(image_name):
    parser = argparse.ArgumentParser(
        description='Train Mask R-CNN to detect balloons.')

    # parser.add_argument("--image", required=False,
    #                     default='test_image_01.tif',
    #                     metavar="<command>",
    #                     help="Image name")

    # args = parser.parse_args()

    # image_name = args.image

    print("start caculating wartermask")
    cal_water_mask(image_name)


    print("start tiling image")
    tile_image(image_name)



    print("start inferencing")
    infer_image(image_name)
    print("start stiching")
    stich_shapefile(image_name)



    process.process_shapefile(image_name)
    
#############################################################
if __name__ == '__main__':
    main()
