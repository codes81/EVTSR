import os
from subprocess import call
import torch
import pandas as pd
import os.path as osp
import pathlib
from abc import ABCMeta, abstractmethod
from gzip import compress
import h5py
import torch
import cv2
import numpy as np
from multiprocessing import Pool
from glob import glob
from basicsr.utils.event_utils import *
import tqdm

class packager():
    """
    Abstract base class for classes that package event-based data to
    some storage format
    """
    __metaclass__ = ABCMeta

    def __init__(self, name, output_path, max_buffer_size=1000000):
        """
        Set class attributes
        @param name The name of the packager (eg: txt_packager)
        @param output_path Where to dump event data
        @param max_buffer_size For packagers that buffer data prior to
            writing, how large this buffer may maximally be
        """
        self.name = name
        self.output_path = output_path
        self.max_buffer_size = max_buffer_size

    @abstractmethod
    def package_events(self, xs, ys, ts, ps):
        """
        Given events, write them to the file/store them into the buffer
        @param xs x component of events
        @param ys y component of events
        @param ts t component of events
        @param ps p component of events
        @returns None
        """
        pass

    @abstractmethod
    def package_image(self, frame, timestamp):
        """
        Given an image, write it to the file/buffer
        @param frame The image frame to write to the file/buffer
        @param timestamp The timestamp of the frame
        @returns None
        """
        pass

    @abstractmethod
    def package_flow(self, flow, timestamp):
        """
        Given an optic flow image, write it to the file/buffer
        @param frame The optic flow image frame to write to the file/buffer
        @param timestamp The timestamp of the optic flow frame
        @returns None
        """
        pass

    @abstractmethod
    def add_metadata(self, num_events, num_pos, num_neg,
                     duration, t0, tk, num_imgs, num_flow):
        """
        Add metadata to the file
        @param num_events The number of events in the sequence
        @param num_pos The numer of positive events in the sequence
        @param num_neg The numer of negative events in the sequence
        @param duration The length of the sequence in seconds
        @param t0 The start time of the sequence
        @param tk The end time of the sequence
        @param num_imgs The number of images in the sequence
        @param num_flow The number of optic flow frames in the sequence
        """
        pass

    @abstractmethod
    def set_data_available(self, num_images, num_flow):
        """
        Configure the file/buffers depending on which data needs to be written
        @param num_images How many images in the dataset
        @param num_flow How many optic flow frames in the dataset
        """
        pass

class hdf5_packager(packager):
    """
    This class packages data to hdf5 files
    """

    def __init__(self, output_path, max_buffer_size=100000000000000):
        packager.__init__(self, 'hdf5', output_path, max_buffer_size)

        self.events_file = h5py.File(output_path, 'w')

    def append_to_dataset(self, dataset, data):
        dataset.resize(dataset.shape[0] + len(data), axis=0)
        if len(data) == 0:
            return
        dataset[-len(data):] = data[:]

    def package_events(self, xs, ys, ts, ps):
        self.append_to_dataset(self.event_xs, xs)
        self.append_to_dataset(self.event_ys, ys)
        self.append_to_dataset(self.event_ts, ts)
        self.append_to_dataset(self.event_ps, ps)

    def package_image(self, image, timestamp, img_idx):
        image_dset = self.events_file.create_dataset("images/{:06d}".format(img_idx),
                                                     data=image, dtype=np.dtype(np.uint8), compression="gzip")
        image_dset.attrs['timestamp'] = timestamp

    def package_voxel(self, txt_file, bins, start, end):

        if not osp.exists(txt_file):
            raise ValueError(f"{txt_file} does not exist!")

        data = pd.read_csv(txt_file, delim_whitespace=True, header=None, names=['t', 'x', 'y', 'p'], dtype={'t': np.float64, 'x': np.int16, 'y': np.int16, 'p': np.int16})
        event_idx = int(osp.splitext(osp.basename(txt_file))[0])

        if data.empty:
            print(f"{txt_file} is empty, will write zero voxel")
            voxel_dset = self.events_file.create_dataset("voxels/{:06d}".format(event_idx), data=np.zeros((bins, 260, 346)), dtype=np.dtype(np.float64), compression="gzip")
            voxel_dset.attrs['is_empty'] = data.empty
            return

        forward_t = torch.from_numpy(data['t'].values)
        forward_x = torch.from_numpy(data['x'].values)
        forward_y = torch.from_numpy(data['y'].values)
        forward_p = torch.from_numpy(data['p'].values)

        if forward_t.size() == torch.Size([]):
            print(f"t.size() is empty. File is {txt_file}")
            h5_file = osp.join('datasets/CED_h5', osp.basename(txt_file.split('/events')) + '.h5')
            os.system(f"rm -rf {h5_file}")
            return

        voxel_f = events_to_voxel_torch(forward_x, forward_y, forward_t, forward_p, bins, device=None, sensor_size=(260, 346))
        normed_voxel_f = voxel_normalization(voxel_f)
        np_voxel_f = normed_voxel_f.numpy()

        self.events_file.create_dataset("voxels_f/{:06d}".format(event_idx), data=np_voxel_f, dtype=np.dtype(np.float64), compression="gzip")

        backward_t = (end - forward_t) + start
        backward_x = forward_x
        backward_y = forward_y
        backward_p = -forward_p

        voxel_b = events_to_voxel_torch(backward_x, backward_y, backward_t, backward_p, bins, device=None, sensor_size=(260, 346))
        normed_voxel_b = voxel_normalization(voxel_b)
        np_voxel_b = normed_voxel_b.numpy()

        self.events_file.create_dataset("voxels_b/{:06d}".format(event_idx), data=np_voxel_b, dtype=np.dtype(np.float64), compression="gzip")

    def package_lr_voxel_forward(self, np_voxel, idx):
        """
            data: ndarray, shape is (Bins, H, W)
            idx: voxel idx
            is_empty:
        """
        self.events_file.create_dataset("voxels_f/{:06d}".format(idx), data = np_voxel, dtype = np.dtype(np.float64), compression="gzip")

    def package_lr_voxel_backward(self, np_voxel, idx):
        """
            data: ndarray, shape is (Bins, H, W)
            idx: voxel idx
            is_empty:
        """
        self.events_file.create_dataset("voxels_b/{:06d}".format(idx), data = np_voxel, dtype = np.dtype(np.float64), compression="gzip")

    def add_metadata(self, duration, t0, tk, num_imgs, num_events, sensor_size):
        self.events_file.attrs['duration'] = duration
        self.events_file.attrs['t0'] = t0
        self.events_file.attrs['tk'] = tk
        self.events_file.attrs['num_imgs'] = num_imgs
        self.events_file.attrs['num_events'] = num_events
        self.events_file.attrs['sensor_resolution'] = sensor_size

    def close(self):
        self.events_file.close()

def make_hdf5_from_folders(
    root_path_list,
    h5_path_list,
    num_bins_list,
    n_thread=40):
    """Make CED to hdf5 format

    Args:
        root_path_list[str]: path contains images, events, timestamps.txt
            eg: root_path_list[0]: datasets/CED/CED_simple/simple_rabbits
        h5_path_list[str]: path to save h5,
            eg: h5_path_list[0]:datasets/CED_h5/simple_rabbits.h5
    
    # pbar = tqdm(total=len(root_path_list), unit=floder)
    # def callback(arg):
    #     get the image data and update pbar.
    #     root, h5_path = arg
    #     pbar.update(1)
    #     pbar.set_description(fRead {osp.basename(root)})

    pool = Pool(n_thread)
    pathlist = []
    for root, h5_path, num_bins in zip(root_path_list, h5_path_list, num_bins_list):
        pathlist.append([root, h5_path, num_bins])

    # for i in range(len(pathlist)):
    #     write_h5_worker(pathlist[i])
    pool.map(write_h5_worker, pathlist)
    pool.close()
    pool.join()
    # pbar.close()
    print(fFinish processing {len(root_path_list)} folders.)


def write_h5_worker(pathlist):
    worker to pacakge images and events

    Args:
        input: root folder contains contains images, events, timestamps.txt
        output: h5_file path

    Example:
        input = datasets/ced/CED/HR/CED_calibration/calib_fluorescent,
        output = datasets/ced/ced_voxels/Voxel_5/HR/calib_fluorescent.h5
        numbins = 5
    
    input, output, num_bins = pathlist[0], pathlist[1], pathlist[2]
    # Step 1: Create output file
    if os.path.exists(output):
        os.remove(output)
    pathlib.Path(osp.dirname(output)).mkdir(exist_ok=True, parents=True)
    file = hdf5_packager(output)

    # Step 2: Pacakge Imgs
    img_paths = sorted(glob(osp.join(input, images/*.png)))
    with open(osp.join(input, timestamp.txt), r) as f:
        timestamp_list = f.read().splitlines()
    f.close()

    for i in range(len(img_paths)):
        image = cv2.imread(img_paths[i])
        timestamp = timestamp_list[i]
        img_idx = int(osp.splitext(osp.basename(img_paths[i]))[0])
        file.package_image(image, timestamp, img_idx)

    # Step 3: package voxel
    events_paths = sorted(glob(osp.join(input, events/*.txt)))
    assert len(timestamp_list) == len(events_paths) + 1
    for i in range(len(events_paths)):
        start = float(timestamp_list[i])
        end = float(timestamp_list[i+1])
        file.package_voxel(txt_file=events_paths[i], bins=num_bins, start = start, end = end)

    # Step 4: Add meta_info to h5 file
    t0 = timestamp_list[0]
    tk = timestamp_list[-1]
    duration = float(tk) - float(t0)
    sensor_size = (260, 346)
    num_imgs = len(img_paths)
    num_voxels = len(events_paths)
    file.add_metadata(duration, t0, tk, num_imgs, num_voxels, sensor_size)
    file.close()

    out_message = osp.basename(output)
    print(fPackage {out_message} finished.)

def make_lr_hdf5_from_folders(
    root_path_list,
    h5_hr_path_list,
    h5_lr_path_list,
    scale_factor_list,
    n_thread=40):
    Make CED to hdf5 format

    Args:
        root_path_list[str]: path contains images, events, timestamps.txt
            eg: root_path_list[0]: datasets/CED/CED_simple/simple_rabbits
        h5_path_list[str]: path to save h5,
            eg: h5_path_list[0]:datasets/CED_h5/simple_rabbits.h5
    
    # pbar = tqdm(total=len(root_path_list), unit=floder)
    # def callback(arg):
    #     get the image data and update pbar.
    #     root, h5_path = arg
    #     pbar.update(1)
    #     pbar.set_description(fRead {osp.basename(root)})

    pool = Pool(n_thread)
    pathlist = []
    for root, hr_h5_path, lr_h5_path, scale_factor in zip(root_path_list, h5_hr_path_list, h5_lr_path_list, scale_factor_list):
        pathlist.append([root, hr_h5_path, lr_h5_path, scale_factor])
    pool.map(write_lr_h5_worker, pathlist)
    pool.close()
    pool.join()
    # pbar.close()
    print(fFinish processing {len(root_path_list)} folders.)

def write_lr_h5_worker(pathlist):
    worker to pacakge images and events

    Args:
        input: root folder contains contains images, events, timestamps.txt
        output: h5_file path

    Example:
        input = [datasets/CED/LR/simple/simple_rabbits, datasets/CED_h5/HR/simple_rabbits.h5]
        output = datasets/CED_h5/LR/simple_rabbits.h5
        scale_factor = 0.5
    
    input, output, scale_factor = pathlist[:2], pathlist[2], pathlist[-1]
    # Step 1: Create output file
    if os.path.exists(output):
        os.remove(output)
    pathlib.Path(osp.dirname(output)).mkdir(exist_ok=True, parents=True)
    file = hdf5_packager(output)

    # Step 2: Pacakge Imgs
    img_paths = sorted(glob(osp.join(input[0], images/*.png)))
    with open(osp.join(input[0], timestamp.txt), r) as f:
        timestamp_list = f.read().splitlines()
    f.close()

    for i in range(len(img_paths)):
        image = cv2.imread(img_paths[i])
        timestamp = timestamp_list[i]
        img_idx = int(osp.splitext(osp.basename(img_paths[i]))[0])
        file.package_image(image, timestamp, img_idx)

    # Step 3: get HR voxel from h5_path, and convert it to lr voxel with bicubic algorighm.
    file2 = h5py.File(input[1], r)
    num_voxels = file2.attrs[num_events]
    for i in range(num_voxels):
        idx = str(i).zfill(6)
        # forward
        voxel_ex_f = torch.from_numpy(file2[fvoxels_f/{idx}][:]).unsqueeze(0)
        voxel_lr_f = torch.nn.functional.interpolate(input = voxel_ex_f, scale_factor = scale_factor, mode = bicubic).squeeze(0).numpy()
        file.package_lr_voxel_forward(np_voxel=voxel_lr_f, idx = i)

        # backward
        voxel_ex_b = torch.from_numpy(file2[fvoxels_b/{idx}][:]).unsqueeze(0)
        voxel_lr_b = torch.nn.functional.interpolate(input = voxel_ex_b, scale_factor = scale_factor, mode = bicubic).squeeze(0).numpy()
        file.package_lr_voxel_backward(np_voxel=voxel_lr_b, idx = i)

    # Step 4: Add meta_info to h5 file
    t0 = timestamp_list[0]
    tk = timestamp_list[-1]
    duration = float(tk) - float(t0)
    sensor_size = (65, 86)
    num_imgs = len(img_paths)
    file.add_metadata(duration, t0, tk, num_imgs, num_voxels, sensor_size)
    file.close()

    out_message = osp.basename(output)
    print(fPackage {out_message} finished.)