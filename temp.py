#!/usr/bin/env python

import sys, os, time, datetime, argparse, threading, signal
from fnmatch import fnmatch
import boto

__version__ = '0.8.1'

version= """
%(prog)s v%(version)s
Copyright (c) 2012 Seth Davis
http://github.com/seedifferently/boto_rsync
"""

description = """
SOURCE and DESTINATION can either be a local path to a directory or specific
file, a custom S3 or GS URL to a directory or specific key in the format of
s3://bucketname/path/or/key, a S3 to S3 transfer using two S3 URLs, or a GS to
GS transfer using two GS URLs.

examples:
    boto-rsync [OPTIONS] /local/path/ s3://bucketname/remote/path/
 or
    boto-rsync [OPTIONS] gs://bucketname/remote/path/or/key /local/path/
 or
    boto-rsync [OPTIONS] s3://bucketname/ s3://another_bucket/
"""

def usage(parser):
    """Prints the usage string and exits."""
    parser.print_help()
    sys.exit(2)

def get_full_path(path):
    """
    Returns a full path with special markers such as "~" and "$USER" expanded.
    """
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    if path and path.endswith(os.sep):
        path = os.path.abspath(path) + os.sep
    else:
        path = os.path.abspath(path)
    return path

def convert_bytes(n):
    """Converts byte sizes into human readable forms such as KB/MB/etc."""
    for x in ['b','K','M','G','T']:
        if n < 1024.0:
            return "%.1f%s" % (n, x)
        n /= 1024.0
    return "%.1f%s" % (n, x)

def spinner(event, every):
    """Animates an ASCII spinner."""
    while True:
        if event.isSet():
            sys.stdout.write('\b \b')
            sys.stdout.flush()
            break
        sys.stdout.write('\b\\')
        sys.stdout.flush()
        event.wait(every)
        sys.stdout.write('\b|')
        sys.stdout.flush()
        event.wait(every)
        sys.stdout.write('\b/')
        sys.stdout.flush()
        event.wait(every)
        sys.stdout.write('\b-')
        sys.stdout.flush()
        event.wait(every)

def submit_cb(bytes_so_far, total_bytes):
    """The "progress" callback for file transfers."""
    global speeds
    
    # Setup speed calculation
    if bytes_so_far < 1:
        speeds = []
        speeds.append((bytes_so_far, time.time()))
    # Skip processing if our last process was less than 850ms ago
    elif bytes_so_far != total_bytes and (time.time() - speeds[-1][1]) < .85:
        return
    
    speeds.append((bytes_so_far, time.time()))
    
    # Try to get ~5 seconds of data info for speed calculation
    s1, t1 = speeds[-1]
    for speed in reversed(speeds):
        s2, t2 = speed
        
        if (t1 - t2) > 5:
            break
    
    # Calculate the speed
    if bytes_so_far == total_bytes:
        # Calculate the overall average speed
        seconds = int(round(speeds[-1][1] - speeds[0][1]))
        if seconds < 1:
            seconds = 1
        speed = 1.0 * total_bytes / seconds
    else:
        # Calculate the current average speed
        seconds = t1 - t2
        if seconds < 1:
            seconds = 1
        size = s1 - s2
        speed = 1.0 * size / seconds
    
    # Calculate the duration
    try:
        if bytes_so_far == total_bytes:
            # Calculate time taken
            duration = int(round(speeds[-1][1] - speeds[0][1]))
        else:
            # Calculate remaining time
            duration = int(round((total_bytes - bytes_so_far) / speed))
        duration = str(datetime.timedelta(seconds=duration))
    except ZeroDivisionError:
        duration = '0:00:00'
    
    # Calculate the progress
    try:
        progress = round((1.0 * bytes_so_far / total_bytes) * 100)
    except ZeroDivisionError:
        progress = 100
    
    sys.stdout.write('    %6s of %6s    %3d%%    %6s/s    %7s    \r' % (
      convert_bytes(bytes_so_far), convert_bytes(total_bytes), progress,
      convert_bytes(speed), duration)
      )
    sys.stdout.flush()

def get_key_name(fullpath, prefix):
    """Returns a key compatible name for a file."""
    key_name = fullpath[len(prefix):]
    l = key_name.split(os.sep)
    key_name = '/'.join(l)
    return key_name.lstrip('/')

def signal_handler(signum, frame):
    """Handles signals."""
    global ev
    
    if signum == signal.SIGINT:
        if ev:
            ev.set()
        
        sys.stdout.write('\n')
        sys.exit(0)

def main():
    global speeds, ev
    
    signal.signal(signal.SIGINT, signal_handler)
    ev = None
    speeds = []
    cb = submit_cb
    num_cb = 10
    rename = False
    copy_file = True
    
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage='%(prog)s [OPTIONS] SOURCE DESTINATION',
        description=description,
        add_help=False
        )
    parser.add_argument(
        '-a', '--access-key', metavar='KEY', dest='cloud_access_key_id',
        help='Your Access Key ID. If not supplied, boto will look for an ' + \
             'environment variable or a credentials file (see README.rst ' + \
             'for more info).'
        )
    parser.add_argument(
        '-s', '--secret-key', metavar='SECRET', dest='cloud_secret_access_key',
        help='Your Secret Key. If not supplied, boto will look for an ' + \
             'environment variable or a credentials file.'
        )
    parser.add_argument(
        '--anon', action='store_true',
        help='Connect without credentials (S3 only). Useful if working ' + \
             'with others\' buckets that have a global read/write ACL.'
        )
    parser.add_argument(
        '--endpoint', metavar='HOST', default='s3.amazonaws.com',
        help='Specify a specific S3 endpoint to connect to via boto\'s ' + \
             '"host" connection argument (S3 only).'
        )
    parser.add_argument(
        '-g', '--grant',
        help='A canned ACL policy that will be granted on each file ' + \
             'transferred to S3/GS. The value provided must be one of the ' + \
             '"canned" ACL policies supported by S3/GS: private, ' + \
             'public-read, public-read-write (S3 only), or authenticated-read'
        )
    parser.add_argument(
        '-m', '--metadata', nargs='+', default=dict(),
        help='One or more "Name: value" pairs specifying what metadata to ' + \
             'set on each file transferred to S3/GS. Note: Be sure to end ' + \
             'your args with "--" if this is the last argument specified ' + \
             'so that SOURCE and DESTINATION can be read properly. e.g. ' + \
             '%(prog)s -m "Content-Type: audio/mpeg" "Content-Disposition: ' + \
             'attachment" -- ./path/ s3://bucket/'
        )
    parser.add_argument(
        '-r', '--reduced', action='store_true',
        help='Enable reduced redundancy on files copied to S3.'
        )
    parser.add_argument(
        '-e', '--encrypt-keys', dest='encrypt', action='store_true',
        help='Enable server-side encryption on files copied to S3 (only ' + \
             'applies when S3 is the destination).'
        )
    parser.add_argument(
        '-p', '--preserve-acl', dest='preserve', action='store_true',
        help='Copy the ACL from the source key to the destination key ' + \
             '(only applies in S3/S3 and GS/GS transfer modes).'
        )
    parser.add_argument(
        '-w', '--no-overwrite', action='store_true',
        help='No files will be overwritten, if the file/key exists on the ' + \
             'destination it will be kept. Note that this is not a sync--' + \
             'even if the file has been updated on the source it will not ' + \
             'be updated on the destination.'
        )
    parser.add_argument(
        '--glob', action='store_true',
        help='Interpret the tail end of SOURCE as a filename pattern and ' + \
             'filter transfers accordingly. Note: If globbing a local ' + \
             'path, make sure that your CLI\'s automatic filename ' + \
             'expansion is disabled (typically accomplished by enclosing ' + \
             'SOURCE in quotes, e.g. "/path/*.zip").'
        )
    parser.add_argument(
        '--no-recurse', action='store_true',
        help='Do not recurse into directories.'
        )
    parser.add_argument(
        '--skip-dirkeys', action='store_true',
        help='When syncing to S3 or GS, skip the creation of keys which ' + \
             'represent "directories" (an empty key ending in "/" for S3 ' + \
             'or "_$folder$" for GS).'
        )
    parser.add_argument(
        '--ignore-empty', action='store_true',
        help='Ignore empty (0-byte) keys/files/directories. This will skip ' + \
             'the transferring of empty directories and keys/files whose ' + \
             'size is 0. Warning: S3/GS often uses empty keys with special ' + \
             'trailing characters to specify directories.'
        )
    parser.add_argument(
        '--delete', action='store_true',
        help='Delete extraneous files from destination dirs after the ' + \
             'transfer has finished (e.g. rsync\'s --delete-after).'
        )
    parser.add_argument(
        '-n', '--dry-run', action='store_true', dest='no_op',
        help='No files will be transferred, but informational messages ' + \
             'will be printed about what would have happened.'
        )
    parser.add_argument(
        '-v', '--verbose', action='store_false', dest='quiet',
        help='Print additional informational messages.'
        )
    parser.add_argument(
        '-d', '--debug', metavar='LEVEL', choices=[0, 1, 2], default=0,
        type=int,
        help='Level 0 means no debug output (default), 1 means normal ' + \
             'debug output from boto, and 2 means boto debug output plus ' + \
             'request/response output from httplib.'
        )
    parser.add_argument(
        '--version', action='version',
        version=version % dict(prog=parser.prog, version=__version__)
        )
    parser.add_argument(
        '-h', '--help', action='help',
        help='show this help message and exit'
        )
    parser.add_argument('SOURCE', help=argparse.SUPPRESS)
    parser.add_argument('DESTINATION', help=argparse.SUPPRESS)
    
    try:
        args = parser.parse_args()
    except argparse.ArgumentTypeError:
        pass
    
    try:
        cloud_access_key_id = args.cloud_access_key_id
        cloud_secret_access_key = args.cloud_secret_access_key
        anon = args.anon
        endpoint = args.endpoint
        grant = args.grant
        metadata = args.metadata
        if not isinstance(metadata, dict):
            metadata = dict([meta.split(': ', 1) for meta in metadata])
        reduced = args.reduced
        encrypt = args.encrypt
        preserve = args.preserve
        no_overwrite = args.no_overwrite
        glob = args.glob
        no_recurse = args.no_recurse or glob
        skip_dirkeys = args.skip_dirkeys
        ignore_empty = args.ignore_empty
        delete = args.delete
        no_op = args.no_op
        quiet = args.quiet
        debug = args.debug
        source = args.SOURCE
        dest = args.DESTINATION
    except:
        sys.stdout.write('\nERROR: Improperly formatted arguments.\n\n')
        usage(parser)
    
    if (source.startswith('s3://') and dest.startswith('gs://') or
        source.startswith('gs://') and dest.startswith('s3://')):
        sys.stdout.write('ERROR: You cannot directly sync between S3 and ' +
                         'Google Storage.\n\n')
        usage(parser)
    elif not source.startswith('s3://') and dest.startswith('s3://'):
        # S3 upload sync
        cloud_service = 's3'
        path = get_full_path(source)
        cloud_bucket = dest[5:].split('/')[0]
        cloud_path = dest[(len(cloud_bucket) + 5):]
        xfer_type = 'upload'
    elif source.startswith('s3://') and not dest.startswith('s3://'):
        # S3 download sync
        cloud_service = 's3'
        cloud_bucket = source[5:].split('/')[0]
        cloud_path = source[(len(cloud_bucket) + 5):]
        path = get_full_path(dest)
        xfer_type = 'download'
    elif not source.startswith('gs://') and dest.startswith('gs://'):
        # GS upload sync
        cloud_service = 'gs'
        path = get_full_path(source)
        cloud_bucket = dest[5:].split('/')[0]
        cloud_path = dest[(len(cloud_bucket) + 5):]
        xfer_type = 'upload'
    elif source.startswith('gs://') and not dest.startswith('gs://'):
        # GS download sync
        cloud_service = 'gs'
        cloud_bucket = source[5:].split('/')[0]
        cloud_path = source[(len(cloud_bucket) + 5):]
        path = get_full_path(dest)
        xfer_type = 'download'
    elif source.startswith('s3://') and dest.startswith('s3://'):
        # S3 to S3 sync
        cloud_service = 's3'
        cloud_bucket = source[5:].split('/')[0]
        cloud_path = source[(len(cloud_bucket) + 5):]
        cloud_dest_bucket = dest[5:].split('/')[0]
        cloud_dest_path = dest[(len(cloud_dest_bucket) + 5):]
        xfer_type = 'sync'
    elif source.startswith('gs://') and dest.startswith('gs://'):
        # GS to GS sync
        cloud_service = 'gs'
        cloud_bucket = source[5:].split('/')[0]
        cloud_path = source[(len(cloud_bucket) + 5):]
        cloud_dest_bucket = dest[5:].split('/')[0]
        cloud_dest_path = dest[(len(cloud_dest_bucket) + 5):]
        xfer_type = 'sync'
    else:
        usage(parser)
    
    # Cloud paths shouldn't have a leading slash
    cloud_path = cloud_path.lstrip('/')
    
    if xfer_type in ['download', 'upload']:
        if not os.path.isdir(path) and not os.path.split(path)[0]:
            sys.stdout.write(
                '\nERROR: %s is not a valid path (does it exist?)\n\n' % path
                )
            usage(parser)
        elif not cloud_bucket or len(cloud_bucket) < 3:
            sys.stdout.write('\nERROR: Bucket name is invalid\n\n')
            usage(parser)
    elif xfer_type in ['sync']:
        if not cloud_bucket or len(cloud_bucket) < 3 and \
           not cloud_dest_bucket or len(cloud_dest_bucket) < 3:
            sys.stdout.write('\nERROR: Bucket name is invalid\n\n')
            usage(parser)
        
        # Cloud paths shouldn't have a leading slash
        cloud_dest_path = cloud_dest_path.lstrip('/')
    
    
    # Connect to Cloud
    if cloud_service == 'gs':
        c = boto.connect_gs(gs_access_key_id=cloud_access_key_id,
                            gs_secret_access_key=cloud_secret_access_key)
    else:
        if anon:
            c = boto.connect_s3(host=endpoint, anon=True)
        else:
            c = boto.connect_s3(aws_access_key_id=cloud_access_key_id,
                                aws_secret_access_key=cloud_secret_access_key,
                                host=endpoint)
    c.debug = debug
    b = c.get_bucket(cloud_bucket)
    if xfer_type in ['sync']:
        b2 = c.get_bucket(cloud_dest_bucket)
    
    
    if xfer_type == 'upload':
        # Perform cloud "upload"
        
        # Check for globbing
        if glob:
            glob = path.split(os.sep)[-1]
            if glob:
                path = path[:-len(glob)]
        
        if os.path.isdir(path) or glob:
            # Possible multi file upload
            sys.stdout.write('Scanning for files to transfer...  ')
            sys.stdout.flush()
            
            if cloud_path and not cloud_path.endswith('/'):
                cloud_path += '/'
            
            # Start "spinner" thread
            ev = threading.Event()
            t1 = threading.Thread(target=spinner, args=(ev, 0.25))
            t1.start()
            
            try:
                keys = {}
                for key in b.list(prefix=cloud_path):
                    if no_recurse and '/' in key.name[len(cloud_path):]:
                        continue
                    
                    if glob and not fnmatch(key.name.split('/')[-1], glob):
                        continue
                    
                    keys[key.name] = key.size
            except Exception, e:
                raise e
            finally:
                # End "spinner" thread
                ev.set()
                t1.join()
                
                # Clean stdout
                sys.stdout.write('\n')
            
            # "Walk" the directory and upload files
            for root, dirs, files in os.walk(path):
                if no_recurse:
                    if root != path:
                        continue
                
                # Create "subdirectories"
                if root != path and not skip_dirkeys:
                    create_dirkey = True
                    
                    if cloud_service == 'gs':
                        key_name = cloud_path + get_key_name(root, path) + \
                                   '_$folder$'
                    else:
                        key_name = cloud_path + get_key_name(root, path) + '/'
                    
                    if ignore_empty and not files:
                        if not quiet:
                            sys.stdout.write(
                                'Skipping %s (empty directory)\n' %
                                key_name.replace('_$folder$', '/')
                                )
                        create_dirkey = False
                    elif key_name in keys:
                        if no_overwrite:
                            if not quiet:
                                sys.stdout.write(
                                    'Skipping %s (not overwriting)\n' %
                                    key_name.replace('_$folder$', '/')
                                    )
                            create_dirkey = False
                        elif key_name.endswith('/') or \
                             key_name.endswith('_$folder$'):
                            if not quiet:
                                sys.stdout.write(
                                    'Skipping %s (size matches)\n' %
                                    key_name.replace('_$folder$', '/')
                                    )
                            create_dirkey = False
                    
                    if create_dirkey:
                        sys.stdout.write(
                            '%s\n' %
                            os.path.join(root[len(path):], '').lstrip(os.sep)
                            )
                        if not no_op:
                            # Setup callback
                            num_cb = 1
                            
                            # Send the directory
                            k = b.new_key(key_name)
                            if cloud_service == 'gs':
                                k.set_contents_from_string(
                                    '', cb=cb, num_cb=num_cb, policy=grant
                                    )
                            else:
                                k.set_contents_from_string(
                                    '', cb=cb, num_cb=num_cb, policy=grant,
                                    reduced_redundancy=reduced,
                                    encrypt_key=encrypt
                                    )
                            keys[key_name] = 0
                            
                            # Clean stdout
                            sys.stdout.write('\n')
                
                for file in files:
                    if glob and not fnmatch(file, glob):
                        continue
                    
                    fullpath = os.path.join(root, file)
                    key_name = cloud_path + get_key_name(fullpath, path)
                    file_size = os.path.getsize(fullpath)
                    
                    if file_size == 0:
                        if ignore_empty:
                            if not quiet:
                                sys.stdout.write(
                                    'Skipping %s (empty file)\n' %
                                    fullpath[len(path):].lstrip(os.sep)
                                    )
                            continue
                    
                    if key_name in keys:
                        if no_overwrite:
                            if not quiet:
                                sys.stdout.write(
                                    'Skipping %s (not overwriting)\n' %
                                    fullpath[len(path):].lstrip(os.sep)
                                    )
                            continue
                        elif keys[key_name] == file_size:
                            if not quiet:
                                sys.stdout.write(
                                    'Skipping %s (size matches)\n' %
                                    fullpath[len(path):].lstrip(os.sep)
                                    )
                            continue
                    
                    sys.stdout.write(
                        '%s\n' %
                        fullpath[len(path):].lstrip(os.sep)
                        )
                    
                    if not no_op:
                        # Setup callback
                        num_cb = int(file_size ** .25)
                        
                        # Send the file
                        k = b.new_key(key_name)
                        k.update_metadata(metadata)
                        if cloud_service == 'gs':
                            k.set_contents_from_filename(
                                fullpath, cb=cb, num_cb=num_cb, policy=grant
                                )
                        else:
                            k.set_contents_from_filename(
                                fullpath, cb=cb, num_cb=num_cb,
                                policy=grant, reduced_redundancy=reduced,
                                encrypt_key=encrypt
                                )
                        keys[key_name] = file_size
                        
                        # Clean stdout
                        sys.stdout.write('\n')
            
            # If specified, perform deletes
            if delete:
                if cloud_path and cloud_path in keys:
                    del(keys[cloud_path])
                
                for root, dirs, files in os.walk(path):
                    if no_recurse:
                        if root != path:
                            continue
                    
                    for file in files:
                        fullpath = os.path.join(root, file)
                        key_name = cloud_path + get_key_name(fullpath, path)
                        if key_name in keys:
                            del(keys[key_name])
                    
                    if root != path:
                        if cloud_service == 'gs':
                            key_name = cloud_path + get_key_name(root, path) + \
                                       '_$folder$'
                        else:
                            key_name = cloud_path + get_key_name(root, path) + \
                                       '/'
                        
                        if key_name in keys:
                            del(keys[key_name])
                
                for key_name, key_size in keys.iteritems():
                    sys.stdout.write(
                        'deleting %s\n' %
                        key_name[len(cloud_path):].replace('_$folder$', '/')
                        )
                    if not no_op:
                        # Delete the key
                        b.delete_key(key_name)
        
        elif os.path.isfile(path):
            # Single file upload
            if cloud_path and not cloud_path.endswith('/'):
                key_name = cloud_path
            else:
                key_name = cloud_path + os.path.split(path)[1]
            filename = os.path.split(path)[1]
            file_size = os.path.getsize(path)
            
            copy_file = True
            key = b.get_key(key_name)
            
            if file_size == 0:
                if ignore_empty:
                    if not quiet:
                        sys.stdout.write(
                            'Skipping %s -> %s (empty file)\n' %
                            filename, key_name.split('/')[-1]
                            )
                    copy_file = False
            
            if key:
                if no_overwrite:
                    copy_file = False
                    if not quiet:
                        if filename != key_name.split('/')[-1]:
                            sys.stdout.write(
                                'Skipping %s -> %s (not overwriting)\n' %
                                filename, key_name.split('/')[-1]
                                )
                        else:
                            sys.stdout.write('Skipping %s (not overwriting)\n' %
                                             filename)
                elif key.size == file_size:
                    copy_file = False
                    if not quiet:
                        if filename != key_name.split('/')[-1]:
                            sys.stdout.write(
                                'Skipping %s -> %s (size matches)\n' %
                                filename, key_name.split('/')[-1]
                                )
                        else:
                            sys.stdout.write('Skipping %s (size matches)\n' %
                                             filename)
            
            if copy_file:
                if filename != key_name.split('/')[-1]:
                    sys.stdout.write('%s -> %s\n' %
                                     (filename, key_name.split('/')[-1]))
                else:
                    sys.stdout.write('%s\n' % filename)
                
                if not no_op:
                    # Setup callback
                    num_cb = int(file_size ** .25)
                    
                    # Send the file
                    k = b.new_key(key_name)
                    k.update_metadata(metadata)
                    if cloud_service == 'gs':
                        k.set_contents_from_filename(
                            path, cb=cb, num_cb=num_cb, policy=grant
                            )
                    else:
                        k.set_contents_from_filename(
                            path, cb=cb, num_cb=num_cb, policy=grant,
                            reduced_redundancy=reduced, encrypt_key=encrypt
                            )
                    
                    # Clean stdout
                    sys.stdout.write('\n')
    
    elif xfer_type == 'download':
        # Perform cloud "download"
        
        cloud_path_key = None
        
        if cloud_path:
            # Check for globbing
            if glob:
                glob = cloud_path.split('/')[-1]
                if glob:
                    cloud_path = cloud_path[:-len(glob)]
            
            if cloud_path:
                cloud_path_key = b.get_key(cloud_path)
        else:
            glob = False
        
        if cloud_path_key and not cloud_path_key.name.endswith('/'):
            # Single file download
            key = cloud_path_key
            keypath = key.name.split('/')[-1]
            if not os.path.isdir(path) and not path.endswith(os.sep):
                rename = True
                fullpath = path
            else:
                fullpath = os.path.join(path, keypath)
            
            if key.size == 0:
                if ignore_empty:
                    if not quiet:
                        if rename:
                            sys.stdout.write(
                                'Skipping %s -> %s (empty key)\n' %
                                keypath, fullpath.split(os.sep)[-1]
                                )
                        else:
                            sys.stdout.write(
                                'Skipping %s (empty key)\n' %
                                fullpath.split(os.sep)[-1]
                                )
                    copy_file = False
            
            if not os.path.isdir(os.path.split(fullpath)[0]):
                if not quiet:
                    sys.stdout.write(
                        'Creating new directory: %s\n' %
                        os.path.split(fullpath)[0]
                        )
                if not no_op:
                    os.makedirs(os.path.split(fullpath)[0])
            elif os.path.exists(fullpath):
                if no_overwrite:
                    if not quiet:
                        if rename:
                            sys.stdout.write(
                                'Skipping %s -> %s (not overwriting)\n' %
                                keypath, fullpath.split(os.sep)[-1]
                                )
                        else:
                            sys.stdout.write(
                                'Skipping %s (not overwriting)\n' %
                                fullpath.split(os.sep)[-1]
                                )
                    copy_file = False
                elif key.size == os.path.getsize(fullpath):
                    if not quiet:
                        if rename:
                            sys.stdout.write(
                                'Skipping %s -> %s (size matches)\n' %
                                keypath.replace('/', os.sep),
                                fullpath.split(os.sep)[-1]
                                )
                        else:
                            sys.stdout.write(
                                'Skipping %s (size matches)\n' %
                                fullpath.split(os.sep)[-1]
                                )
                    copy_file = False
            
            if copy_file:
                if rename:
                    sys.stdout.write(
                        '%s -> %s\n' % (keypath, fullpath.split(os.sep)[-1])
                        )
                else:
                    sys.stdout.write('%s\n' % keypath)
                
                if not no_op:
                    # Setup callback
                    num_cb = int(key.size ** .25)
                    
                    # Get the file
                    key.get_contents_to_filename(fullpath, cb=cb, num_cb=num_cb)
                    
                    # Clean stdout
                    sys.stdout.write('\n')
        
        else:
            # Possible multi file download
            if not cloud_path_key and cloud_path and \
               not cloud_path.endswith('/'):
                cloud_path += '/'
            
            keys = []
            
            sys.stdout.write('Scanning for keys to transfer...\n')
            
            for key in b.list(prefix=cloud_path):
                # Skip the key if it is the cloud path
                if not key.name[len(cloud_path):] or \
                   key.name[len(cloud_path):] == '$folder$':
                    continue
                
                if no_recurse and '/' in key.name[len(cloud_path):]:
                    continue
                
                if glob and not fnmatch(key.name.split('/')[-1], glob):
                    continue
                
                keypath = key.name[len(cloud_path):]
                if cloud_service == 'gs':
                    fullpath = os.path.join(
                        path,
                        keypath.replace('_$folder$', os.sep)
                        )
                else:
                    fullpath = os.path.join(path, keypath.replace('/', os.sep))
                
                keys.append(fullpath)
                
                if key.size == 0 and ignore_empty:
                    if not quiet:
                        sys.stdout.write(
                            'Skipping %s (empty key)\n' %
                            fullpath[len(os.path.join(path, '')):]
                            )
                    continue
                
                if not os.path.isdir(os.path.split(fullpath)[0]):
                    if not quiet:
                        sys.stdout.write(
                            'Creating new directory: %s\n' %
                            os.path.split(fullpath)[0]
                            )
                    if not no_op:
                        os.makedirs(os.path.split(fullpath)[0])
                elif os.path.exists(fullpath):
                    if no_overwrite:
                        if not quiet:
                            sys.stdout.write(
                                'Skipping %s (not overwriting)\n' %
                                fullpath[len(os.path.join(path, '')):]
                                )
                        continue
                    elif key.size == os.path.getsize(fullpath) or \
                         key.name.endswith('/') or \
                         key.name.endswith('_$folder$'):
                        if not quiet:
                            sys.stdout.write(
                                'Skipping %s (size matches)\n' %
                                fullpath[len(os.path.join(path, '')):]
                                )
                        continue
                
                if cloud_service == 'gs':
                    sys.stdout.write('%s\n' %
                                     keypath.replace('_$folder$', os.sep))
                else:
                    sys.stdout.write('%s\n' % keypath.replace('/', os.sep))
                
                if not no_op:
                    if key.name.endswith('/') or key.name.endswith('_$folder$'):
                        # Looks like a directory, so just print the status
                        submit_cb(0, 0)
                    else:
                        # Setup callback
                        num_cb = int(key.size ** .25)
                        
                        # Get the file
                        key.get_contents_to_filename(fullpath, cb=cb,
                                                     num_cb=num_cb)
                    
                    # Clean stdout
                    sys.stdout.write('\n')
            
            # If specified, perform deletes
            if delete:
                for root, dirs, files in os.walk(path):
                    if no_recurse:
                        if root != path:
                            continue
                    
                    if files:
                        for file in files:
                            if glob and not fnmatch(file, glob):
                                continue
                            
                            filepath = os.path.join(root, file)
                            if filepath not in keys:
                                sys.stdout.write(
                                    'deleting %s\n' %
                                    filepath[len(os.path.join(path, '')):]
                                    )
                                if not no_op:
                                    # Delete the file
                                    os.remove(filepath)
                    elif root != path:
                        dirpath = os.path.join(root, '')
                        if dirpath not in keys:
                            sys.stdout.write(
                                'deleting %s\n' %
                                dirpath[len(os.path.join(path, '')):]
                                )
                            if not no_op:
                                # Remove the directory
                                os.rmdir(dirpath)
    else:
        # Perform cloud to cloud "sync"
        
        cloud_path_key = None
        
        if cloud_path:
            # Check for globbing
            if glob:
                glob = cloud_path.split('/')[-1]
                if glob:
                    cloud_path = cloud_path[:-len(glob)]
            
            if cloud_path:
                cloud_path_key = b.get_key(cloud_path)
        else:
            glob = False
        
        if cloud_path_key and not cloud_path_key.name.endswith('/'):
            # Single file sync
            key = cloud_path_key
            keypath = key.name.split('/')[-1]
            if cloud_dest_path and not cloud_dest_path.endswith('/'):
                rename = True
                fullpath = cloud_dest_path
            else:
                fullpath = cloud_dest_path + keypath
                fullpath = fullpath.lstrip('/')
            
            dest_key = b2.get_key(fullpath)
            
            if key.size == 0:
                if ignore_empty:
                    if not quiet:
                        if rename:
                            sys.stdout.write(
                                'Skipping %s -> %s (empty key)\n' %
                                keypath.split('/')[-1], fullpath.split('/')[-1]
                                )
                        else:
                            sys.stdout.write(
                                'Skipping %s (empty key)\n' % fullpath
                                )
                    copy_file = False
            
            if dest_key:
                # TODO: Check for differing ACL
                if no_overwrite:
                    if not quiet:
                        if rename:
                            sys.stdout.write(
                                'Skipping %s -> %s (not overwriting)\n' %
                                keypath.split('/')[-1], fullpath.split('/')[-1]
                                )
                        else:
                            sys.stdout.write(
                                'Skipping %s (not overwriting)\n' % fullpath
                                )
                    copy_file = False
                elif key.size == dest_key.size:
                    if not quiet:
                        if rename:
                            sys.stdout.write(
                                'Skipping %s -> %s (size matches)\n' %
                                keypath.split('/')[-1], fullpath.split('/')[-1]
                                )
                        else:
                            sys.stdout.write(
                                'Skipping %s (size matches)\n' % fullpath
                                )
                    copy_file = False
            
            if copy_file:
                if rename:
                    sys.stdout.write('%s -> %s...  ' % (
                        keypath.split('/')[-1], fullpath.split('/')[-1])
                        )
                else:
                    sys.stdout.write('%s...  ' % keypath)
                sys.stdout.flush()
                if not no_op:
                    speeds.append((0, time.time()))
                    
                    # Start "spinner" thread
                    ev = threading.Event()
                    t1 = threading.Thread(target=spinner, args=(ev, 0.25))
                    t1.start()
                    
                    try:
                        # Transfer the key
                        key.copy(cloud_dest_bucket, fullpath,
                                 metadata=metadata, reduced_redundancy=reduced,
                                 preserve_acl=preserve, encrypt_key=encrypt)
                    except Exception, e:
                        raise e
                    finally:
                        # End "spinner" thread
                        ev.set()
                        t1.join()
                    
                    if rename:
                        sys.stdout.write('\r%s -> %s    \n' % (
                            keypath.split('/')[-1], fullpath.split('/')[-1]
                            ))
                    else:
                        sys.stdout.write('\r%s    \n' % keypath)
                    sys.stdout.flush()
                    submit_cb(key.size, key.size)
                else:
                    if rename:
                        sys.stdout.write('\r%s -> %s    ' % (
                            keypath.split('/')[-1], fullpath.split('/')[-1])
                            )
                    else:
                        sys.stdout.write('\r%s    ' % keypath)
                    sys.stdout.flush()
                
                # Clean stdout
                sys.stdout.write('\n')
        
        else:
            # Possible multi file sync
            if not cloud_path_key and cloud_path and \
               not cloud_path.endswith('/'):
                cloud_path += '/'
            if cloud_dest_path and not cloud_dest_path.endswith('/'):
                cloud_dest_path += '/'
            
            keys = []
            
            sys.stdout.write('Scanning for keys to transfer...\n')
            
            for key in b.list(prefix=cloud_path):
                if no_recurse and '/' in key.name[len(cloud_path):]:
                    continue
                
                if glob and not fnmatch(key.name.split('/')[-1], glob):
                    continue
                
                if key.name == cloud_path:
                    keypath = key.name.split('/')[-2] + '/'
                else:
                    keypath = key.name[len(cloud_path):]
                fullpath = cloud_dest_path + keypath
                fullpath = fullpath.lstrip('/')
                
                keys.append(fullpath)
                dest_key = b2.get_key(fullpath)
                
                if key.size == 0:
                    if ignore_empty:
                        if not quiet:
                            sys.stdout.write(
                                'Skipping %s (empty key)\n' %
                                fullpath.replace('_$folder$', '/')
                                )
                        continue
                
                if dest_key:
                    # TODO: Check for differing ACL
                    if no_overwrite:
                        if not quiet:
                            sys.stdout.write(
                                'Skipping %s (not overwriting)\n' %
                                fullpath.replace('_$folder$', '/')
                                )
                        continue
                    elif key.size == dest_key.size:
                        if not quiet:
                            sys.stdout.write(
                                'Skipping %s (size matches)\n' %
                                fullpath.replace('_$folder$', '/')
                                )
                        continue
                
                sys.stdout.write('%s...  ' % keypath.replace('_$folder$', '/'))
                sys.stdout.flush()
                if not no_op:
                    speeds.append((0, time.time()))
                    
                    # Start "spinner" thread
                    ev = threading.Event()
                    t1 = threading.Thread(target=spinner, args=(ev, 0.25))
                    t1.start()
                    
                    try:
                        # Transfer the key
                        key.copy(cloud_dest_bucket, fullpath,
                                 metadata=metadata, reduced_redundancy=reduced,
                                 preserve_acl=preserve, encrypt_key=encrypt)
                    except Exception, e:
                        raise e
                    finally:
                        # End "spinner" thread
                        ev.set()
                        t1.join()
                    
                    sys.stdout.write('\r%s    \n' % \
                                     keypath.replace('_$folder$', '/'))
                    sys.stdout.flush()
                    submit_cb(key.size, key.size)
                else:
                    sys.stdout.write('\r%s    ' % \
                                     keypath.replace('_$folder$', '/'))
                    sys.stdout.flush()
                
                # Clean stdout
                sys.stdout.write('\n')
            
            # If specified, perform deletes
            if delete:
                for key in b2.list(prefix=cloud_dest_path):
                    if no_recurse and '/' in key.name[len(cloud_dest_path):]:
                        continue
                    
                    if glob and not fnmatch(key.name.split('/')[-1], glob):
                        continue
                    
                    keypath = key.name[len(cloud_dest_path):]
                    
                    if key.name not in keys:
                        sys.stdout.write(
                            'deleting %s\n' % keypath.replace('_$folder$', '/')
                            )
                        if not no_op:
                            # Delete the key
                            key.delete()

if __name__ == "__main__":
    main()
