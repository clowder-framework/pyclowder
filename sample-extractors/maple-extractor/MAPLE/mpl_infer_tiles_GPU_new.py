#!/usr/bin/python3

import time
import queue
import multiprocessing
import shapefile
from skimage.measure import find_contours
from mpl_config import MPL_Config
import os
import h5py
import skimage.draw
import numpy as np


class Predictor(multiprocessing.Process):
    def __init__(self, input_queue, gpu_id,
                          POLYGON_DIR,
                          weights_path,
                          output_shp_root,
                          x_resolution,
                          y_resolution,len_imgs,image_name):

        multiprocessing.Process.__init__(self)
        self.input_queue = input_queue
        self.gpu_id = gpu_id
        self.len_imgs = len_imgs

        self.POLYGON_DIR = POLYGON_DIR
        self.weights_path = weights_path
        self.output_shp_root = output_shp_root

        self.x_resolution = x_resolution
        self.y_resolution = y_resolution
        self.image_name = image_name

    def run(self):

        # --------------------------- Preseting --------------------------- 
        # import regular module
        import os
        import sys
        import numpy as np
        import tensorflow as tf
        import shapefile
        from mpl_config import MPL_Config
        from mpl_config import PolygonConfig
        from collections import defaultdict
        from shapely.geometry import Polygon, Point

        # Root directory of the project
        ROOT_DIR = MPL_Config.ROOT_DIR
        MY_WEIGHT_FILE = MPL_Config.WEIGHT_PATH

        # Import Mask RCNN
        sys.path.append(ROOT_DIR)  ### Chandi: root is getting updated

        # Directory to save logs and trained model
        MODEL_DIR = os.path.join(ROOT_DIR, "local_dir/datasets/logs")


        import model as modellib

        # --------------------------- Configurations ---------------------------
        # Set config
        config = PolygonConfig()

        output_shp_root = self.output_shp_root
        
        # --------------------------- Preferences ---------------------------
        # Device to load the neural network on.
        # Useful if you're training a model on the same 
        # machine, in which case use CPU and leave the
        # GPU for training.
        DEVICE = "/gpu:%s"%(self.gpu_id)  # /cpu:0 or /gpu:0
        os.environ['CUDA_VISIBLE_DEVICES'] = "{}".format(self.gpu_id)

        # Inspect the model in training or inference modes
        # values: 'inference' or 'training'
        # TODO: code for 'training' test mode not ready yet
        TEST_MODE = "inference"
        

        # Create model in inference mode
        with tf.device(DEVICE):
            model = modellib.MaskRCNN(mode="inference", model_dir=MODEL_DIR,
                                      config=config)
        
        # Load weights
        print("Loading weights ", MODEL_DIR)
        model.load_weights(MY_WEIGHT_FILE, by_name=True)
        output_shp_name_1 = output_shp_root.split('/')[-1]

        ##model.load_weights(MY_WEIGHT_FILE, by_name=True, exclude=["mrcnn_class_logits", "mrcnn_bbox_fc","mrcnn_bbox", "mrcnn_mask"])
        temp_name = "%s_%d.shp"%(output_shp_name_1, self.gpu_id)

        output_path_1 = os.path.join(output_shp_root, temp_name)
        w_final = shapefile.Writer(output_path_1)

        w_final.field('Class', 'C', size=5)
        w_final.field("Sensor", 'C', size=10)
        w_final.field("Date", 'C', size=15)
        w_final.field("Time", 'C', size=15)
        w_final.field("Image", 'C', size=100)
        w_final.field("Area", "N", decimal=3)
        w_final.field("CentroidX", "N", decimal=3)
        w_final.field("CentroidY", "N", decimal=3)
        w_final.field("Perimeter", "N", decimal=3)
        w_final.field("Length", "N", decimal=3)
        w_final.field("Width", "N", decimal=3)


        #w_final.field('Sensor', 'C', size=5)
        count =0
        total = self.len_imgs
        # --------------------------- Workers --------------------------- 

        total_tiles = 0
        dict_polygons = defaultdict(dict)
        while True:
            job_data = self.input_queue.get()
            count += 1

            if job_data is None:
                self.input_queue.task_done()
                print("Exiting Process %d" % self.gpu_id)
                break

            else:
                # get the upper left x y of the image

                i = int(job_data[0][0])
                j = int(job_data[0][1])
                ul_row_divided_img = job_data[0][2]
                ul_col_divided_img = job_data[0][3]
                tile_no = job_data[0][4]
                image = job_data[1]
                #print(f"{i},{j},{ul_row_divided_img},{ul_col_divided_img}")

                #output_shp_name = "%s_%s_%s_%s.shp" % (i, j, ul_row_divided_img, ul_col_divided_img)
                #output_shp_path = os.path.join(output_shp_root, output_shp_name)

                results = model.detect([image], verbose=False)

                r = results[0]
                #polygon_list_size = np.zeros(len(r['class_ids']))

                if len(r['class_ids']):
                    count_p = 0

                    for id_masks in range(r['masks'].shape[2]):

                        # read the mask
                        mask = r['masks'][:, :, id_masks]
                        padded_mask = np.zeros(
                            (mask.shape[0] + 2, mask.shape[1] + 2), dtype=np.uint8)
                        padded_mask[1:-1, 1:-1] = mask
                        class_id = r['class_ids'][id_masks]

                        try:
                            contours = find_contours(padded_mask, 0.5, 'high')[0] * np.array(
                                [[self.y_resolution, self.x_resolution]])
                            contours = contours + np.array([[float(ul_row_divided_img), float(ul_col_divided_img)]])
                            # swap two cols
                            contours.T[[0, 1]] = contours.T[[1, 0]]
                            # write shp file
                            w_final.poly([contours.tolist()])

                            poly = Polygon(contours.tolist())
                            area = poly.area
                            perimeter = poly.length
                            box = poly.minimum_rotated_rectangle
                            x, y = box.exterior.coords.xy
                            centroid = poly.centroid
                            #
                            p0 = Point(x[0], y[0])
                            p1 = Point(x[1], y[1])
                            p2 = Point(x[2], y[2])
                            edge_lenth = (p0.distance(p1), p1.distance(p2))
                            length = max(edge_lenth)
                            width = min(edge_lenth)

                            #w_final.record(Class=class_id,Sensor='WV02')
                            w_final.record(Class=class_id, Sensor=self.image_name[0:4],Date=self.image_name[5:13],
                                           Time=self.image_name[13:19],Image=self.image_name[:-4],Area=area,
                                           CentroidX=centroid.x,CentroidY=centroid.y,Perimeter=perimeter,
                                           Length=length,Width=width)



                        except:
                            contours = []
                            pass

                        count_p += 1


                dict_polygons[int(tile_no)] = [r['masks'].shape[2]]


                if (MPL_Config.LOGGING):
                    print(f"## {count} of {total} ::: {len(r['class_ids'])}  $$$$ {r['class_ids']}")
                    sys.stdout.flush()

        import pickle
        worker_root = MPL_Config.WORKER_ROOT
        db_file_path = os.path.join(worker_root, "neighbors/%s_polydict_%d.pkl" % (self.image_name,self.gpu_id))
        dbfile = open(db_file_path, 'wb')
        pickle.dump(dict_polygons, dbfile)
        dbfile.close()
        w_final.close()




def inference_image(POLYGON_DIR,
                    weights_path,
                    output_shp_root,
                    file1,file2,image_name):

    f1 = h5py.File(file1, 'r')
    f2 = h5py.File(file2, 'r')

    values = f2.get('values')
    n1 = np.array(values)
    x_resolution = n1[0]
    y_resolution = n1[1]
    len_imgs = n1[2]

    # The number of GPU you want to use
    num_gpus = MPL_Config.NUM_GPUS_PER_CORE

    input_queue = multiprocessing.JoinableQueue()

    p_list = []

    for i in range(0, num_gpus):
        # set the i as the GPU device you want to use

        p = Predictor(input_queue, i,
                      POLYGON_DIR,
                      weights_path,
                      output_shp_root,
                      x_resolution,
                      y_resolution,
                      len_imgs,image_name)
        p_list.append(p)




    for p in p_list:
        p.start()



    for img in range(int(len_imgs)):
        image = f1.get(f"image_{img+1}")
        params = f2.get(f"param_{img+1}")
        img_stack = np.array(image)
        img_data = (np.array(params))

        job = [img_data,img_stack]

        input_queue.put(job)
        #print(input_queue.qsize())
    f1.close()
    f2.close()


    for i in range(num_gpus):
        input_queue.put(None)

    for p in p_list:
        p.join()
