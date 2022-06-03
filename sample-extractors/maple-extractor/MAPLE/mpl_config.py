
from config import Config
class MPL_Config(object):
    # ROOT_DIR = r'/mnt/data/Rajitha/MAPLE/MAPLE_2'
    ROOT_DIR = r'/app'
    OVL_SHAPEFILE = 'selection_207_208_209_223_224.shp'
    ## Do not change this section
    #-----------------------------------------------------------------
    INPUT_IMAGE_DIR = ROOT_DIR + r'/data/input_img'
    DIVIDED_IMAGE_DIR = ROOT_DIR + r'/data/divided_img'
    OUTPUT_SHP_DIR = ROOT_DIR + r'/data/output_shp'
    FINAL_SHP_DIR = ROOT_DIR + r'/data/final_shp'
    WATER_MASK_DIR = ROOT_DIR + r'/data/water_mask'
    WATER_MASK_Final_DIR = ROOT_DIR + r'/data/water_mask_final'
    TEMP_W_IMG_DIR = ROOT_DIR + r'/data/water_mask/temp'
    OUTPUT_IMAGE_DIR = ROOT_DIR + r'/data/output_img'
    WORKER_ROOT =  ROOT_DIR + r'/data'
    OVERLAP_SHAPE_DIR = ROOT_DIR + r'/data/overlaps/shapefiles'
    PROJECTED_SHP_DIR = ROOT_DIR + r'/data/projected_shp'
    # weight_name = r'trained_weights_Dataset_017_15_0.h5'
    #weight_name = r'trained_weights_234_001.h5'
    # weight_name = r'trained_weights_Dataset_187_12_8.h5'
    # weight_name =  r'trained_weights_Dataset_179_9_33.h5'
    #weight_name = r'mask_rcnn_trained_weights_dataset_0.001000_194_19_18__0023.h5'
    #weight_name = r'trained_weights_Dataset_215_12_38_.h5'
    #weight_name = r'trained_weights_Dataset_239_9_13_.h5'
    # weight_name = r'trained_weights_Dataset_245_16_59_.h5'
    weight_name = r'rajita_trained_weights_Dataset_251_13_24_.h5'


    #WEIGHT_PATH = ROOT_DIR + weight_name
    #WEIGHT_PATH = ROOT_DIR + weight_name
    #WEIGHT_PATH = ROOT_DIR + weight_name
    #WEIGHT_PATH = ROOT_DIR + weight_name
    #WEIGHT_PATH = ROOT_DIR + weight_name
    WEIGHT_PATH = ROOT_DIR + r"/" + weight_name
    #-----------------------------------------------------------------
    CROP_SIZE = 256

    LOGGING = True
    NUM_GPUS_PER_CORE = 1



class PolygonConfig(Config):
    """Configuration for training on the toy dataset.
    Derives from the base Config class and overrides some values.
    """
    # Give the configuration a recognizable name
    NAME = "ice_wedge_polygon"

    # We use a GPU with 12GB memory, which can fit two images.
    # Adjust down if you use a smaller GPU.
    IMAGES_PER_GPU = 1

    # Number of classes (including background)
    NUM_CLASSES = 1 + 1 + 1  # Background + highcenter + lowcenter

    # Number of training steps per epoch
    STEPS_PER_EPOCH = 340

    # Skip detections with < 70% confidence
    DETECTION_MIN_CONFIDENCE = 0.3

    # Max number of final detections
    DETECTION_MAX_INSTANCES = 200
    # Non-maximum suppression threshold for detection
    DETECTION_NMS_THRESHOLD = 0.3
    RPN_NMS_THRESHOLD = 0.8
    IMAGE_MIN_DIM = 256
    IMAGE_MAX_DIM = 256

