#!/usr/bin/env python3

"""
PI: Chandi Witharana
Author: Rajitha Udwalpola
Starting date: 2020-03-29
"""

import os.path
import shutil
import datetime
import os
from mpl_config import MPL_Config
import mpl_stitchshpfile_new as stich
import sys

# work tag
WORKTAG = 1
DIETAG = 0



def main(input_img_name):

    sys.path.append(MPL_Config.ROOT_DIR)

    crop_size = MPL_Config.CROP_SIZE

    # worker roots
    worker_img_root = MPL_Config.INPUT_IMAGE_DIR
    worker_divided_img_root = MPL_Config.DIVIDED_IMAGE_DIR
    worker_finaloutput_root =  MPL_Config.FINAL_SHP_DIR
    worker_output_shp_root = MPL_Config.OUTPUT_SHP_DIR
    #input image path
    input_img_path = os.path.join(worker_img_root, input_img_name)

    # Create subfolder for each image
    new_file_name = input_img_name.split('.tif')[0]
    worker_divided_img_subroot = os.path.join(worker_divided_img_root, new_file_name)
    worker_finaloutput_subroot = os.path.join(worker_finaloutput_root, new_file_name)
    worker_output_shp_subroot = os.path.join(worker_output_shp_root, new_file_name)
    print(worker_divided_img_subroot)

    try:
        shutil.rmtree(worker_divided_img_subroot)
    except:
        print("director deletion failed")
        pass
    os.mkdir(worker_divided_img_subroot)



    stich.stitch_shapefile(worker_output_shp_subroot,
                            worker_finaloutput_subroot, new_file_name,new_file_name)

    return "done Divide"



if __name__ == '__main__':
    import argparse
    #############################################################
    parser = argparse.ArgumentParser(
        description='Train Mask R-CNN to detect balloons.')

    parser.add_argument("--image", required=False,
                        default='WV02_20100702212747_1030010005B6B800_10JUL02212747-M1BS-500083220090_01_P001_u16rf3413_pansh.tif',
                        metavar="<command>",
                        help="Image name")

    args = parser.parse_args()


    main(args.image)
