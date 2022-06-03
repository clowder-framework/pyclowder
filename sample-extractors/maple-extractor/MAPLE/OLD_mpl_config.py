import numpy as np

from config import Config
class MPL_Config(object):
    # ROOT_DIR = r'/mnt/data/Rajitha/MAPLE/MAPLE_2'
    ROOT_DIR = r'/home/kastan/ncsa/MAPLE_Local'
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
#


class PolygonConfig(MPL_Config): # used to inherit from: Config
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
    
    # KASTAN ADDED THIS, MAKES ME NERVOUS
    IMAGE_RESIZE_MODE = "square"
    IMAGE_CHANNEL_COUNT = 3
    
    # Input image size
    if IMAGE_RESIZE_MODE == "crop":
        IMAGE_SHAPE = np.array([IMAGE_MIN_DIM, IMAGE_MIN_DIM, IMAGE_CHANNEL_COUNT])
    else:
        IMAGE_SHAPE = np.array([IMAGE_MAX_DIM, IMAGE_MAX_DIM, IMAGE_CHANNEL_COUNT])
    
    # FROM MAPLE_XSEDE/config.;y
    # Image meta data length
    # See compose_image_meta() for details
    IMAGE_META_SIZE = 1 + 3 + 3 + 4 + 1 + NUM_CLASSES
    # IMAGE_META_SIZE = 256
    
    # MROE THINGS FRON MAPLE_XSEDE/config.py
    # Backbone network architecture
    # Supported values are: resnet50, resnet101.
    # You can also provide a callable that should have the signature
    # of model.resnet_graph. If you do so, you need to supply a callable
    # to COMPUTE_BACKBONE_SHAPE as well
    BACKBONE = "resnet101"

    # Only useful if you supply a callable to BACKBONE. Should compute
    # the shape of each layer of the FPN Pyramid.
    # See model.compute_backbone_shapes
    COMPUTE_BACKBONE_SHAPE = None

    # The strides of each layer of the FPN Pyramid. These values
    # are based on a Resnet101 backbone.
    BACKBONE_STRIDES = [4, 8, 16, 32, 64]

    # Size of the fully-connected layers in the classification graph
    FPN_CLASSIF_FC_LAYERS_SIZE = 1024

    # Size of the top-down layers used to build the feature pyramid
    TOP_DOWN_PYRAMID_SIZE = 256

    # Number of classification classes (including background)
    NUM_CLASSES = 1  # Override in sub-classes

    # Length of square anchor side in pixels
    RPN_ANCHOR_SCALES = (32, 64, 128, 256, 512)

    # Ratios of anchors at each cell (width/height)
    # A value of 1 represents a square anchor, and 0.5 is a wide anchor
    RPN_ANCHOR_RATIOS = [0.5, 1, 2]

    # Anchor stride
    # If 1 then anchors are created for each cell in the backbone feature map.
    # If 2, then anchors are created for every other cell, and so on.
    RPN_ANCHOR_STRIDE = 1

    # Non-max suppression threshold to filter RPN proposals.
    # You can increase this during training to generate more propsals.
    RPN_NMS_THRESHOLD = 0.7

    # How many anchors per image to use for RPN training
    RPN_TRAIN_ANCHORS_PER_IMAGE = 256

    # ROIs kept after tf.nn.top_k and before non-maximum suppression
    PRE_NMS_LIMIT = 6000

    # ROIs kept after non-maximum suppression (training and inference)
    POST_NMS_ROIS_TRAINING = 2000
    POST_NMS_ROIS_INFERENCE = 1000

    # If enabled, resizes instance masks to a smaller size to reduce
    # memory load. Recommended when using high-resolution images.
    USE_MINI_MASK = True
    MINI_MASK_SHAPE = (56, 56)  # (height, width) of the mini-mask

    # Image mean (RGB)
    MEAN_PIXEL = np.array([123.7, 116.8, 103.9])

    # Number of ROIs per image to feed to classifier/mask heads
    # The Mask RCNN paper uses 512 but often the RPN doesn't generate
    # enough positive proposals to fill this and keep a positive:negative
    # ratio of 1:3. You can increase the number of proposals by adjusting
    # the RPN NMS threshold.
    TRAIN_ROIS_PER_IMAGE = 200

    # Percent of positive ROIs used to train classifier/mask heads
    ROI_POSITIVE_RATIO = 0.33

    # Pooled ROIs
    POOL_SIZE = 7
    MASK_POOL_SIZE = 14

    # Shape of output mask
    # To change this you also need to change the neural network mask branch
    MASK_SHAPE = [28, 28]

    # Maximum number of ground truth instances to use in one image
    MAX_GT_INSTANCES = 100

    # Bounding box refinement standard deviation for RPN and final detections.
    RPN_BBOX_STD_DEV = np.array([0.1, 0.1, 0.2, 0.2])
    BBOX_STD_DEV = np.array([0.1, 0.1, 0.2, 0.2])

    # Max number of final detections
    DETECTION_MAX_INSTANCES = 100

    # Minimum probability value to accept a detected instance
    # ROIs below this threshold are skipped
    DETECTION_MIN_CONFIDENCE = 0.7

    # Non-maximum suppression threshold for detection
    DETECTION_NMS_THRESHOLD = 0.3

    # Learning rate and momentum
    # The Mask RCNN paper uses lr=0.02, but on TensorFlow it causes
    # weights to explode. Likely due to differences in optimizer
    # implementation.
    LEARNING_RATE = 0.001
    LEARNING_MOMENTUM = 0.9

    # Weight decay regularization
    WEIGHT_DECAY = 0.0001

    # Loss weights for more precise optimization.
    # Can be used for R-CNN training setup.
    LOSS_WEIGHTS = {
        "rpn_class_loss": 1.,
        "rpn_bbox_loss": 1.,
        "mrcnn_class_loss": 1.,
        "mrcnn_bbox_loss": 1.,
        "mrcnn_mask_loss": 1.
    }

    # Use RPN ROIs or externally generated ROIs for training
    # Keep this True for most situations. Set to False if you want to train
    # the head branches on ROI generated by code rather than the ROIs from
    # the RPN. For example, to debug the classifier head without having to
    # train the RPN.
    USE_RPN_ROIS = True

    # Train or freeze batch normalization layers
    #     None: Train BN layers. This is the normal mode
    #     False: Freeze BN layers. Good when using a small batch size
    #     True: (don't use). Set layer in training mode even when predicting
    TRAIN_BN = False  # Defaulting to False since batch size is often small

    # Gradient norm clipping
    GRADIENT_CLIP_NORM = 5.0
    
    BATCH_SIZE = 1
    
    GPU_COUNT = 1


