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
import mpl_divideimg_234 as divide
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

    return "done Divide"



if __name__ == '__main__':
    import argparse
    #############################################################
    parser = argparse.ArgumentParser(
        description='Train Mask R-CNN to detect balloons.')

    parser.add_argument("--image", required=False,
                        default='test_image_01.tif',
                        metavar="<command>",
                        help="Image name")

    args = parser.parse_args()


    main(args.image)
