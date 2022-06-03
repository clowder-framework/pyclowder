import sys
from mpl_config import MPL_Config
import  os
from osgeo import osr, gdal,ogr
import shapefile as shp
import glob
import pickle


def main(imgs_path):

    image_search =os.path.join(imgs_path,"*.tif")
    image_files = sorted(glob.glob(image_search))

    output_shp_file = os.path.join(imgs_path,"image_bounds.shp")
    outDriver = ogr.GetDriverByName('ESRI Shapefile')
    if os.path.exists(output_shp_file):
        os.remove(output_shp_file)


    w_final = shp.Writer(output_shp_file)
    w_final.field('Name', 'C', size=200)
    image_dict = {}
    for idx , image in enumerate(image_files):
        #input_image_path = os.path.join(imgs_path,image)
        im_name = image.split('/')[-1]
        IMG1 = gdal.Open(image)
        gt1 = IMG1.GetGeoTransform()
        image_dict[im_name] = idx

        ulx, x_resolution, _, uly, _, y_resolution = gt1

        band_1_raster = IMG1.GetRasterBand(1)

        YSize = IMG1.RasterYSize
        XSize = IMG1.RasterXSize

        brx = ulx + x_resolution * XSize
        bry = uly + y_resolution * YSize

        # create output file


        vertices = []
        vertices.append([ulx, uly])
        vertices.append([ulx, bry])
        vertices.append([brx, bry])
        vertices.append([brx, uly])
        vertices.append([ulx, uly])
        w_final.record(im_name)
        w_final.poly([vertices])
    w_final.close()

    db_file_path = os.path.join(MPL_Config.WORKER_ROOT, "footprint/SyntheticOverlaps/image_dict.pkl")
    dbfile = open(db_file_path, 'wb')
    pickle.dump(image_dict, dbfile)
    dbfile.close()

    dbfile = open(db_file_path, 'rb')
    mydict = pickle.load(dbfile)
    dbfile.close()

    print("Done creating shape file")
if __name__ == '__main__':
    sys.path.append(MPL_Config.ROOT_DIR)
    data_root = MPL_Config.WORKER_ROOT
    imgs_path = os.path.join(data_root, "footprint/SyntheticOverlaps")
    main(imgs_path)