from  mgrs import MGRS
import os
from os.path import join as _join
from os.path import split as _split
from os.path import exists as _exists
import datetime
import requests
import htmllistparse
from urllib.request import urlopen
from pyhdf.SD import SD, SDC
from osgeo import gdal, osr

from subprocess import Popen

import numpy as np

gdal.UseExceptions()


def isint(x):
    try:
        return float(int(x)) == float(x)
    except:
        return False


class HLS2Manager(object):
    def __init__(self):
        self._m = MGRS()

    def query(self, mgrs, sat='L', year=None, version='v1.4', startdate=None, enddate=None):
        sat = sat.upper()
        assert sat in 'LS'

        if year is None:
            year = datetime.datetime.now().year

        assert isint(year)
        year = int(year)

        zone = mgrs[:2]
        grid = mgrs[2]
        aa_x, aa_y = tuple(mgrs[3:5])

        url = 'https://hls.gsfc.nasa.gov/data/{version}/{sat}30/{year}/{zone}/{grid}/{aa_x}/{aa_y}/'\
              .format(version=version,
                      sat=sat,
                      year=year,
                      zone=zone,
                      grid=grid,
                      aa_x=aa_x, aa_y=aa_y)

        cwd, listing = htmllistparse.fetch_listing(url)

        listing = [item.name for item in listing if item.name.endswith('hdf')]
        return listing

    def identify_mgrs_from_point(self, lng=None, lat=None):
        return self._m.toMGRS(latitude=lat, longitude=lng, MGRSPrecision=0)

    def identify_mgrs_from_bbox(self, bbox):
        l, t, r, b = bbox
        assert l < r
        assert b < t

        delta = 0.01

        mgrss = set()
        for lng in np.arange(l, r, delta):
            mgrss.add(self._m.toMGRS(latitude=t, longitude=lng, MGRSPrecision=0))
            mgrss.add(self._m.toMGRS(latitude=b, longitude=lng, MGRSPrecision=0))

        for lat in np.arange(b, t, delta):
            mgrss.add(self._m.toMGRS(latitude=lat, longitude=l, MGRSPrecision=0))
            mgrss.add(self._m.toMGRS(latitude=lat, longitude=r, MGRSPrecision=0))

        return tuple(mgrss)

    def retrieve(self, identifier, datadir='.'):
        assert identifier.startswith('HLS')
        assert identifier.endswith('.hdf')

        _identifier = identifier[:-4].split('.')
        sat = _identifier[1]
        zone = _identifier[2][1:3]
        grid = _identifier[2][3]
        aa_x, aa_y = _identifier[2][4], _identifier[2][5]
        _date = _identifier[3]
        year = _date[:4]
        version = '.'.join(_identifier[4:])

        url = 'https://hls.gsfc.nasa.gov/data/{version}/{sat}/{year}/{zone}/{grid}/{aa_x}/{aa_y}/{identifier}'\
              .format(version=version,
                      sat=sat,
                      year=year,
                      zone=zone,
                      grid=grid,
                      aa_x=aa_x, aa_y=aa_y,
                      identifier=identifier)

        output = urlopen(url, timeout=60)
        with open(_join(datadir, identifier), 'wb') as fp:
            fp.write(output.read())

        output = urlopen(url + '.hdr', timeout=60)
        with open(_join(datadir, identifier + '.hdr'), 'wb') as fp:
            fp.write(output.read())


class HLS2(object):
    def __init__(self, identifier):
        _identifier = _split(identifier)
        self.identifier = _identifier[-1]
        path = identifier

        assert _exists(path)
        self.path = path
        self.file = file =  SD(path, SDC.READ)
        _variables = {}
        for short_name in file.datasets().keys():
            band = file.select(short_name)
            attrs = band.attributes()
            if 'long_name' in attrs:
                _variables[attrs['long_name']] = short_name
            elif 'QA description' in attrs:
                _variables['QA'] = short_name
            else:
                raise NotImplementedError()

        self._variables = _variables

    @property
    def sat(self):
        return self.identifier.split('.')[1][0]

    @property
    def variables(self):
        return list(self._variables.keys())

    @property
    def ncols(self):
        return int(self.file.attributes()['NCOLS'])

    @property
    def nrows(self):
        return int(self.file.attributes()['NROWS'])

    @property
    def ulx(self):
        return float(self.file.attributes()['ULX'])

    @property
    def uly(self):
        return float(self.file.attributes()['ULY'])

    @property
    def spatial_resolution(self):
        return float(self.file.attributes()['SPATIAL_RESOLUTION'])

    @property
    def transform(self):
        return [self.ulx, self.spatial_resolution, 0.0, self.uly, 0.0, -self.spatial_resolution]

    @property
    def _tileid_key(self):
        key = 'TILE_ID'
        if self.sat.startswith('L'):
            key = 'SENTINEL2_TILEID'
        return key

    @property
    def utm_zone(self):
        return int(self.identifier[9:11])

    @property
    def grid(self):
        return self.identifier[8]

    @property
    def is_north(self):
        grid = self.grid
        assert grid in 'ABCDEFGHJKLMNPQRSTUVWXYZ'
        return grid in 'NPQRSTUVWXYZ'

    @property
    def hdr_fn(self):
        return self.path + '.hdr'

    @property
    def geog_cs(self):
        horizontal_cs_name = self.file.attributes()['HORIZONTAL_CS_NAME']

        if 'WGS84' in horizontal_cs_name:
            return 'WGS84'
        elif 'NAD27' in horizontal_cs_name:
            return 'NAD27'
        else:
            raise NotImplementedError()

    def _get_band_add_offset(self, band):
        if band not in self._variables:
            return 0.0

        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        try:
            return float(_attrs['add_offset'])
        except KeyError:
            return None

    def _get_band_scale_factor(self, band):
        if band not in self._variables:
            return 0.0001

        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        try:
            return float(_attrs['scale_factor'])
        except KeyError:
            return None

    def _get_band_fill_value(self, band):
        if band not in self._variables:
            return -1000

        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        dtype = self._get_band_dtype(band)
        try:
            return dtype(_attrs['_FillValue'])
        except KeyError:
            return None

    def _get_band_dtype(self, band):
        if band not in self._variables:
            return np.int16

        _band = self.file.select(self._variables[band])
        return getattr(np, str(_band.get().dtype))

    def _unpack_band(self, band):
        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        add_offset = self._get_band_add_offset(band)
        scale_factor = self._get_band_scale_factor(band)
        fill_value = self._get_band_fill_value(band)

        _data = _band.get()
        if fill_value is not None:
            _data = np.ma.masked_values(_data, fill_value)

        if add_offset is not None and scale_factor is not None:
            _data = (_data - add_offset) * scale_factor

        return _data

    @property
    def red(self):
        return self._unpack_band('Red')

    @property
    def green(self):
        return self._unpack_band('Green')

    @property
    def blue(self):
        return self._unpack_band('Blue')

    @property
    def nir(self):
        if self.sat.startswith('L'):
            # 845 - 885 nm
            # https://landsat.gsfc.nasa.gov/landsat-8/landsat-8-bands/
            return self._unpack_band('NIR')

        elif self.sat.startswith('S'):
            # https://en.wikipedia.org/wiki/Sentinel-2
            # central wavelength of 864.7 and 21 nm bandwidth
            return self._unpack_band('NIR_Narrow')

    @property
    def swir1(self):
        return self._unpack_band('SWIR1')

    @property
    def swir2(self):
        return self._unpack_band('SWIR2')

    @property
    def tirs1(self):
        return self._unpack_band('TIRS1')

    @property
    def tirs2(self):
        return self._unpack_band('TIRS2')

    @property
    def qa(self):
        return self._unpack_band('QA')

    @property
    def ndvi(self):
        """https://www.usgs.gov/land-resources/nli/landsat/landsat-normalized-difference-vegetation-index?qt-science_support_page_related_con=0#qt-science_support_page_related_con"""
        nir = self.nir
        red = self.red

        return (nir - red) / (nir + red)

    def export_band(self, band, as_float=True, compress=True):

        if as_float:
            _data = getattr(self, band)
            dtype = np.float32
        else:
            add_offset = self._get_band_add_offset(band)
            scale_factor = self._get_band_scale_factor(band)
            dtype = self._get_band_dtype(band)

            _data = getattr(self, band)
            _data = np.ma.array((_data / scale_factor) - add_offset, dtype=dtype)

        fill_value = self._get_band_fill_value(band)

        gdal_type = {np.float32: gdal.GDT_Float32,
                     np.float64: gdal.GDT_Float64,
                     np.int16: gdal.GDT_Int16,
                     np.uint8: gdal.GDT_Byte}[dtype]

        driver = gdal.GetDriverByName('GTiff')
        fname = tmp_fname = '{}-{}.tif'.format(self.identifier[:-4], band)

        if compress:
            tmp_fname = fname[:-4] + '.tmp.tif'

        ds = driver.Create(tmp_fname, self.nrows, self.ncols, 1, gdal_type)
        ds.SetGeoTransform(self.transform)
        srs = osr.SpatialReference()
        srs.SetUTM(self.utm_zone, (0, 1)[self.is_north])
        srs.SetWellKnownGeogCS(self.geog_cs)
        ds.SetProjection(srs.ExportToWkt())
        _band = ds.GetRasterBand(1)
        _band.WriteArray(_data)

        if fill_value is not None:
            _band.SetNoDataValue(fill_value)

        ds = None

        if compress:
            cmd = ['gdal_translate', '-co', 'compress=DEFLATE', '-co', 'zlevel=9', tmp_fname, fname]

            _log = open(fname + '.err', 'w')
            p = Popen(cmd, stdout=_log, stderr=_log)
            p.wait()
            _log.close()

            if _exists(fname):
                os.remove(fname + '.err')
                os.remove(tmp_fname)


if __name__ == "__main__":
    hls_manager = HLS2Manager()
    _mgrs = hls_manager.identify_mgrs_from_bbox(bbox=[-117.8270, 46.0027, -116.5416, 45.3048])

    print(_mgrs)
    import sys
    sys.exit()

    identifier = 'data/HLS.L30.T11TNN.2020007.v1.4.hdf'

    hls = HLS(identifier)
    print(hls.variables)

    hls2 = HLS('data/HLS.S30.T11TNN.2020280.v1.4.hdf')
    print(hls2.variables)

    swir1 = hls.export_band('ndvi', as_float=False)
    swir1 = hls2.export_band('ndvi', as_float=False)

    import sys
    sys.exit()

    hls_manager = HLSManager()
    _mgrs = hls_manager.identify_mgrs_from_point(lng=-116, lat=47)

    print(_mgrs)

    listing = hls_manager.query(mgrs=_mgrs, sat='S')
    print(listing)

    # hls_manager.retrieve('HLS.L30.T11TNN.2020007.v1.4.hdf')
    hls_manager.retrieve('HLS.S30.T11TNN.2020280.v1.4.hdf')
