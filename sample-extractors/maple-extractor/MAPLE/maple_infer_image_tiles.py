import os
import shutil
#from parsl.data_provider.files import File
import os.path
import mpl_infer_tiles_GPU as inference

def main(input_img_name):


    from mpl_config import MPL_Config
    imgs_path = MPL_Config.INPUT_IMAGE_DIR
    worker_root = MPL_Config.WORKER_ROOT

    # worker root
    worker_divided_img_root = MPL_Config.DIVIDED_IMAGE_DIR #os.path.join(worker_root, "divided_img")
    worker_output_shp_root = MPL_Config.OUTPUT_SHP_DIR#os.path.join(worker_root, "infer_shp")


    # Create subfolder for each image
    new_file_name = input_img_name.split('.tif')[0]
    worker_divided_img_subroot = os.path.join(worker_divided_img_root, new_file_name)
    worker_output_shp_subroot = os.path.join(worker_output_shp_root, new_file_name)

    file1 = (os.path.join(worker_divided_img_subroot, 'image_data.h5'))
    file2 = (os.path.join(worker_divided_img_subroot, 'image_param.h5'))

    try:
        shutil.rmtree(worker_output_shp_subroot)

    except:
        print("director deletion failed")
        pass
        # check local storage for temporary storage
    os.mkdir(worker_output_shp_subroot)

    print(worker_output_shp_subroot)
    # path in the module
    POLYGON_DIR = worker_root
    weights_path = MPL_Config.WEIGHT_PATH

    inference.inference_image(POLYGON_DIR,
                         weights_path,
                             worker_output_shp_subroot, file1, file2)





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
