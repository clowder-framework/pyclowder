
import shutil
import argparse


import os.path
import shutil
import datetime
import os
from mpl_config import MPL_Config
import mpl_infer_tiles_GPU_new as inference
import sys
# work tag
WORKTAG = 1
DIETAG = 0






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






#############################################################
parser = argparse.ArgumentParser(
    description='Train Mask R-CNN to detect balloons.')

parser.add_argument("--image", required=False,
                    default='test_image_01.tif',
                    metavar="<command>",
                    help="Image name")

args = parser.parse_args()

image_name = args.image
print("start inferencing")
infer_image(image_name)

