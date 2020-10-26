import sys
import argparse
from hls2 import *

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Retrieve HLS-2')
    parser.add_argument('--hls_datadir', type=str)
    parser.add_argument('--identifiers', type=str, nargs='+')
    parser.add_argument('--lat_lng', type=float, nargs=2)
    parser.add_argument('--bbox', type=float, nargs=4)
    parser.add_argument('--query', action='store_true')
    parser.add_argument('-L', action='store_true')
    parser.add_argument('-S', action='store_true')
    parser.add_argument('--year', type=int)
    parser.add_argument('--start_date', type=str)
    parser.add_argument('--end_date', type=str)

    parser.add_argument('--bands', type=str, nargs='+')
    parser.add_argument('--out_dir',  type=str)
    parser.add_argument('--force_utm_zone',  type=int, nargs=1)
    parser.add_argument('--as_float', action='store_true')
    parser.add_argument('--nocompress', action='store_true')

    parser.add_argument('--merge_and_crop', action='store_true')

    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()

    debug = args.debug
    verbose = args.verbose

    if debug:
        print(args)

    if args.hls_datadir is not None:
        hls_manager = HLS2Manager(datadir=args.hls_datadir)
    else:
        hls_manager = HLS2Manager(datadir='/geodata/hls/')

    sat = None
    if args.L:
        sat = 'L'
    elif args.S:
        sat = 'S'

    identifiers = args.identifiers
    if identifiers is None:
        identifiers = []

    mgrss = []

    if args.lat_lng is not None:
        lat, lng = args.lat_lng
        mgrss.append(hls_manager.identify_mgrs_from_point(lng=lng, lat=lat))

    if args.bbox is not None:
        bbox = args.bbox
        mgrss.extend(hls_manager.identify_mgrs_from_bbox(bbox=bbox))

    if verbose:
        print('mgrss={}'.format(mgrss))

    for mgrs in mgrss:
        identifiers.extend(hls_manager.query(mgrs, sat=sat,
                                             year=args.year,
                                             start_date=args.start_date,
                                             end_date=args.end_date))

    if verbose:
        print('identifiers: {}'.format(identifiers))

    if args.query:
        sys.exit()

    if not args.merge_and_crop:
        for i, identifier in enumerate(identifiers):
            identifier_path = hls_manager.retrieve(identifier)
            if verbose:
                print('retrieved', identifier_path, '({} of {})'.format(i+1, len(identifiers)))

            if args.bands is not None:
                hls = hls_manager.get_hls(identifier)
                for band in args.bands:
                    hls.export_band(band, as_float=args.as_float, compress=not args.nocompress,
                                    out_dir=args.out_dir, force_utm_zone=args.force_utm_zone)

                    if verbose:
                        print('extracted', identifier, ':', band)

    else:
        if args.bands is not None and args.bbox is not None:
            d = {}
            for identifier in identifiers:
                _date = identifier.split('.')[3]
                if _date not in d:
                    d[_date] = []

                d[_date].append(identifier)

            for _date, _identifiers in d.items():
                if verbose:
                    print('merging', _identifiers)

                hls_manager.merge_and_crop(_identifiers, bands=args.bands, bbox=args.bbox, as_float=args.as_float,
                                           out_dir=args.out_dir, verbose=verbose)

