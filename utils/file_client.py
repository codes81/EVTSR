
import numpy as np
from abc import ABCMeta, abstractmethod
import os.path as osp

class BaseStorageBackend(metaclass=ABCMeta):
    """Abstract class of storage backends.

    All backends need to implement two apis: ``get()`` and ``get_text()``.
    ``get()`` reads the file as a byte stream and ``get_text()`` reads the file
    as texts.
    

    @abstractmethod
    def get(self, filepath):
        pass

    @abstractmethod
    def get_text(self, filepath):
        pass


class MemcachedBackend(BaseStorageBackend):
    Memcached storage backend.

    Attributes:
        server_list_cfg (str): Config file for memcached server list.
        client_cfg (str): Config file for memcached client.
        sys_path (str | None): Additional path to be appended to `sys.path`.
            Default: None.
    

    def __init__(self, server_list_cfg, client_cfg, sys_path=None):
        if sys_path is not None:
            import sys
            sys.path.append(sys_path)
        try:
            import mc
        except ImportError:
            raise ImportError(Please install memcached to enable MemcachedBackend.)

        self.server_list_cfg = server_list_cfg
        self.client_cfg = client_cfg
        self._client = mc.MemcachedClient.GetInstance(self.server_list_cfg, self.client_cfg)
        # mc.pyvector servers as a point which points to a memory cache
        self._mc_buffer = mc.pyvector()

    def get(self, filepath):
        filepath = str(filepath)
        import mc
        self._client.Get(filepath, self._mc_buffer)
        value_buf = mc.ConvertBuffer(self._mc_buffer)
        return value_buf

    def get_text(self, filepath):
        raise NotImplementedError


class HardDiskBackend(BaseStorageBackend):
    Raw hard disks storage backend.

    def get(self, filepath):
        filepath = str(filepath)
        with open(filepath, rb) as f:
            value_buf = f.read()
        return value_buf

    def get_text(self, filepath):
        filepath = str(filepath)
        with open(filepath, r) as f:
            value_buf = f.read()
        return value_buf


class LmdbBackend(BaseStorageBackend):
    Lmdb storage backend.

    Args:
        db_paths (str | list[str]): Lmdb database paths.
        client_keys (str | list[str]): Lmdb client keys. Default: default.
        readonly (bool, optional): Lmdb environment parameter. If True,
            disallow any write operations. Default: True.
        lock (bool, optional): Lmdb environment parameter. If False, when
            concurrent access occurs, do not lock the database. Default: False.
        readahead (bool, optional): Lmdb environment parameter. If False,
            disable the OS filesystem readahead mechanism, which may improve
            random read performance when a database is larger than RAM.
            Default: False.

    Attributes:
        db_paths (list): Lmdb database path.
        _client (list): A list of several lmdb envs.
    

    def __init__(self, db_paths, client_keys=default, readonly=True, lock=False, readahead=False, **kwargs):
        try:
            import lmdb
        except ImportError:
            raise ImportError(Please install lmdb to enable LmdbBackend.)

        if isinstance(client_keys, str):
            client_keys = [client_keys]

        if isinstance(db_paths, list):
            self.db_paths = [str(v) for v in db_paths]
        elif isinstance(db_paths, str):
            self.db_paths = [str(db_paths)]
        assert len(client_keys) == len(self.db_paths), (client_keys and db_paths should have the same length, 
                                                        fbut received {len(client_keys)} and {len(self.db_paths)}.)

        self._client = {}
        for client, path in zip(client_keys, self.db_paths):
            self._client[client] = lmdb.open(path, readonly=readonly, lock=lock, readahead=readahead, **kwargs)

    def get(self, filepath, client_key):
        Get values according to the filepath from one lmdb named client_key.

        Args:
            filepath (str | obj:`Path`): Here, filepath is the lmdb key.
            client_key (str): Used for distinguishing different lmdb envs.
        
        filepath = str(filepath)
        assert client_key in self._client, (fclient_key {client_key} is not in lmdb clients.)
        client = self._client[client_key]
        with client.begin(write=False) as txn:
            value_buf = txn.get(filepath.encode(ascii))
        return value_buf

    def get_text(self, filepath):
        raise NotImplementedError

class Hdf5Backend(BaseStorageBackend):

    def __init__(self, h5_paths, client_keys=default, h5_clip=default, is_event=None, is_bidirectional=None, center_frame_only=None, is_pretrain=None, name=None, **kwargs):
        try:
            import h5py
        except ImportError:
            raise ImportError(Please install h5py to enable Hdf5Backend.)

        if isinstance(client_keys, str):
            client_keys = [client_keys]

        if isinstance(h5_paths, list):
            self.h5_paths = [str(v) for v in h5_paths]
        elif isinstance(h5_paths, str):
            self.h5_paths = [str(h5_paths)]
        assert len(client_keys) == len(self.h5_paths), (client_keys and db_paths should have the same length, 
                                                        fbut received {len(client_keys)} and {len(self.h5_paths)}.)

        self._client = {}
        self.is_event = is_event
        self.is_bidirectional = is_bidirectional
        self.center_frame_only = center_frame_only
        self.is_pretrain = is_pretrain
        self.name = name
        for client, path in zip(client_keys, self.h5_paths):
            # print(/osp.join(path, h5_clip): , osp.join(path, h5_clip))
            try:
                self._client[client] = h5py.File(osp.join(path, h5_clip), r,libver=latest, swmr=True)
            except Exception:
                print(fIO error, please check {path} {h5_clip}.)

    def get(self, filepath):

        if self.is_pretrain:
            assert len(filepath) == 2
            file_lr = self._client[LR]
            I1 = file_lr[fimages/{filepath[0]:06d}][:].astype(np.float32) / 255.
            I2 = file_lr[fimages/{filepath[1]:06d}][:].astype(np.float32) / 255.
            if vimeo in self.name.lower():
                voxel = file_lr[fvoxels/{filepath[0]:06d}][:].astype(np.float32)
            elif ced in self.name.lower() or vid4 in self.name.lower() or reds in self.name.lower():
                voxel = file_lr[fvoxels_f/{filepath[0]:06d}][:].astype(np.float32)
            return I1, I2, voxel

        else:
            ## filepath is neighor_list contains num_frame image keys
            ## get LQ
            # print(self._client.keys())
            file_lr = self._client[LR]
            file_hr = self._client[HR]    
            img_lrs = []
            img_lrss = []
            img_hrs = []
            length = len(filepath)
            if self.center_frame_only:
                # only vimeo need it
                img_hr = file_hr[fimages/{3:06d}][:].astype(np.float32) / 255.
                img_hrs.append(img_hr)
                for idx in filepath:
                    img_lr = file_lr[fimages/{idx:06d}][:].astype(np.float32) / 255.
                    img_lrs.append(img_lr)
            else:
                for idx in filepath:
                    #img_lr = file_lr[fimages/{idx:06d}][:].astype(np.float32) / 255.
                    img_lr = file_lr[fimages/img_{(idx+1):08d}][:].astype(np.float32) / 255.
                    img_lrss.append(img_lr)
                img_lrs.append(np.mean(img_lrss, axis=0)) #算average的lr

                    #img_hr = file_hr[fimages/{idx:06d}][:].astype(np.float32) / 255.
                #img_hr = file_hr[fimages/image_hr_1][:].astype(np.float32) / 255.
                image_keys = list(file_hr[images].keys())
                first_image_key = image_keys[0]
                img_hr = file_hr[fimages/{first_image_key}][:].astype(np.float32) / 255.
                img_hrs.append(img_hr)

            if self.is_event:  #这里肯定就是加voxels的地方
                # for voxels with bidirectionalional data
                event_lqs = []
                if self.is_bidirectional:
                    voxels_f = []
                    voxels_b = []
                    if vimeo in self.name.lower():
                        for idx in filepath[:-1]:
                            voxel_f = file_lr[fvoxels/{idx:06d}][:].astype(np.float32)
                            voxel_b = file_lr[fvoxels/{(11 - idx):06d}][:].astype(np.float32)
                            voxels_f.append(voxel_f)
                            voxels_b.append(voxel_b)
                        event_lqs.extend(voxels_f)
                        event_lqs.extend(voxels_b)

                    elif ced in self.name.lower() or vid4 in self.name.lower() or reds in self.name.lower():
                        for idx in filepath[:-1]:
                            voxel_f = file_lr[fvoxels_f/0{idx:09d}][:].astype(np.float32)
                            voxel_b = file_lr[fvoxels_b/0{idx:09d}][:].astype(np.float32)
                            voxels_f.append(voxel_f)
                            voxels_b.append(voxel_b)
                        event_lqs.extend(voxels_f)
                        event_lqs.extend(voxels_b)
                # for voxels without bidirectionalional data
                else:
                    for idx in filepath[:-1]:
                        event_lq = file_lr[fvoxels/{idx:06d}][:].astype(np.float32)
                        event_lqs.append(event_lq)

                return img_lrs, img_hrs, event_lqs

            return img_lrs, img_hrs

    def get_text(self, filepath):
        raise NotImplementedError


class FileClient(object):
    A general file client to access files in different backend.

    The client loads a file or text in a specified backend from its path
    and return it as a binary file. it can also register other backend
    accessor with a given name and backend class.

    Attributes:
        backend (str): The storage backend type. Options are disk,
            memcached and lmdb.
        client (:obj:`BaseStorageBackend`): The backend object.
    

    _backends = {
        disk: HardDiskBackend,
        memcached: MemcachedBackend,
        lmdb: LmdbBackend,
        hdf5: Hdf5Backend,
    }

    def __init__(self, backend=disk, **kwargs):
        if backend not in self._backends:
            raise ValueError(fBackend {backend} is not supported. Currently supported ones
                             f are {list(self._backends.keys())})
        self.backend = backend
        self.client = self._backends[backend](**kwargs)

    def get(self, filepath, client_key=default):
        # client_key is used only for lmdb, where different fileclients have
        # different lmdb environments.
        if self.backend == lmdb:
            return self.client.get(filepath, client_key)
        else:
            return self.client.get(filepath)

    def get_text(self, filepath):
        return self.client.get_text(filepath)
