import sys
from hls2 import *

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Extract hdf to GeoTIFF')
    parser.add_argument('identifiers', type=str, nargs='+')
    parser.add_argument('--bands', type=str, nargs='+')
    parser.add_argument('--out_dir',  type=str)
    parser.add_argument('--force_utm_zone',  type=int, nargs=1)
    parser.add_argument('--as_float', action='store_true')
    parser.add_argument('--compress', action='store_true')

    args = parser.parse_args()

    print(args)

    identifiers = args.identifiers
    bands = args.bands

    hls_manager = HLS2Manager(datadir='/geodata/hls/')

    for identifier in identifiers:
        hls = hls_manager.get_hls(identifier)

        for band in bands:
            hls.export_band(band, as_float=args.as_float, compress=args.compress,
                            out_dir=args.out_dir, force_utm_zone=args.force_utm_zone)
