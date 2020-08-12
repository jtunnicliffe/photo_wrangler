try:
    import PIL
    import PIL.Image as PILimage
    from PIL import ImageDraw, ImageFont, ImageEnhance
    from PIL.ExifTags import TAGS, GPSTAGS
    import sys
    import os
    import shutil
    import glob
    import re
    import datetime
    import shutil
    import hashlib
    import pyodbc
except ImportError as err:
    exit(err)


class ImageData(object):
    '''
        Class to extract image EXIF data
    '''
    def __init__(self, img):
        self.img = img
        self.get_exif_data()
        self.lat = self.get_lat()
        self.lon = self.get_lon()
        self.date =self.get_date_time()
        self.origdate = self.get_orig_date_time()
        self.digidate = self.get_digi_date_time()
        self.earliest_date = self.get_earliest_exif_date()
        super(ImageData, self).__init__()
    
    @staticmethod
    def get_if_exist(data, key):
        if key in data:
            return data[key]
        return None
    
    @staticmethod
    def convert_to_degrees(value):
        """
            Helper function to convert the GPS coordinates
            stored in the EXIF to degrees in float format
        """
        d0 = value[0][0]
        d1 = value[0][1]
        d = float(d0) / float(d1)
        m0 = value[1][0]
        m1 = value[1][1]
        m = float(m0) / float(m1)
        
        s0 = value[2][0]
        s1 = value[2][1]
        s = float(s0) / float(s1)
        
        return d + (m / 60.0) + (s / 3600.0)
    
    def get_exif_data(self):
        """
            Returns a dictionary from the exif data of an PIL Image item. Also
            converts the GPS Tags
        """
        try:
            exif_data = {}
            info = self.img._getexif()
            if info:
                for tag, value in info.items():
                    decoded = TAGS.get(tag, tag)
                    if decoded == "GPSInfo":
                        gps_data = {}
                        for t in value:
                            sub_decoded = GPSTAGS.get(t, t)
                            gps_data[sub_decoded] = value[t]
                        exif_data[decoded] = gps_data
                    else:
                        exif_data[decoded] = value
            self.exif_data = exif_data
        except:
            self.exif_data = None
        # return exif_data
    
    def get_lat(self):
        """
            Returns the latitude and longitude, if available, from the 
            provided exif_data (obtained through get_exif_data above)
        """
        # print(exif_data)
        if self.exif_data and 'GPSInfo' in self.exif_data:
            gps_info = self.exif_data["GPSInfo"]
            gps_latitude = self.get_if_exist(gps_info, "GPSLatitude")
            gps_latitude_ref = self.get_if_exist(gps_info, 'GPSLatitudeRef')
            if gps_latitude and gps_latitude_ref:
                lat = self.convert_to_degrees(gps_latitude)
                if gps_latitude_ref != "N":
                    lat = 0 - lat
                lat = str(f"{lat:.{5}f}")
                return lat
        else:
            return None
    
    def get_lon(self):
        """
            Returns the latitude and longitude, if available, from the 
            provided exif_data (obtained through get_exif_data above)
        """
        # print(exif_data)
        if self.exif_data and 'GPSInfo' in self.exif_data:
            gps_info = self.exif_data["GPSInfo"]
            gps_longitude = self.get_if_exist(gps_info, 'GPSLongitude')
            gps_longitude_ref = self.get_if_exist(gps_info, 'GPSLongitudeRef')
            if gps_longitude and gps_longitude_ref:
                lon = self.convert_to_degrees(gps_longitude)
                if gps_longitude_ref != "E":
                    lon = 0 - lon
                lon = str(f"{lon:.{5}f}")
                return lon
        else:
            return None
    
    def get_date_time(self):
        if self.exif_data and 'DateTime' in self.exif_data:
            date_and_time = self.exif_data['DateTime']
            return date_and_time
    
    def get_orig_date_time(self):
        if self.exif_data and 'DateTimeOriginal' in self.exif_data:
            date_and_time = self.exif_data['DateTimeOriginal']
            return date_and_time
    
    def get_digi_date_time(self):
        if self.exif_data and 'DateTimeDigitized' in self.exif_data:
            date_and_time = self.exif_data['DateTimeDigitized']
            return date_and_time
    
    def get_earliest_exif_date(self):
        dt = self.get_date_time()
        odt = self.get_orig_date_time()
        ddt = self.get_digi_date_time()
        if (dt is None or (odt is not None and odt < dt)):
            dt = odt
        if (dt is None or (ddt is not None and ddt < dt)):
            dt = ddt
        return dt


class ArchiveMgr(object):
    '''
        Class for managing archive folder tree, preventing duplicate entries, etc.
        It maintains a HashDict, which is a cached data structure reflecting the
        names and hashes of each file stored in each subfolder (yyyy\mm bucket).
        This cache is automatically rehydrated when a request is made and the
        files on disk do not reflect the current cache. This is relatively easy
        since we don't need to deal with deletions, just additions.
    '''
    NULLHASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    def __init__(self, root, hashdict=None):
        self.Root = root
        if (hashdict):
            self.HashDict = hashdict
        else:
            self.HashDict = dict()
    
    def submit_file_for_backup(self, infile, bucket, hash=None):
        '''
        The method will see if its cache is current, and if not (i.e., there
        are files on disk not in our HashDict), it will hash any unknown
        files and add them to HashDict. If the hash of the input file matches
        that of any file already archived, this is a duplicate, and will be
        rejected with DUPE_ENTRY. Otherwise, the file will be copied to the
        archive tree in the bucket specified (avoiding any name collisions),
        and the new file name and hash will be added to the cached HashDict.
        :param infile: fully-qualified name of file to add
        :param bucket: yyyy\mm bucket (subfolder) to archive under
        :param hash: file hash (optional), will hash ourselves if not provided
        :return: list of [status, stored_file_name
        '''
        # if hash is provided (perhaps we already knew it due to earlier
        # workflow), just use it, otherwise hash it ourselves.
        if (hash is None):
            hash = self.hash_file(infile)
        # Get hash dictionary for this bucket
        if (bucket in self.HashDict):
            hashes = self.HashDict[bucket]
        else:
            # this is a new yyyy\mm bucket, perhaps
            if (self._is_valid_bucket(bucket)):
                # create element in HashDict for this new bucket
                self.HashDict[bucket] = dict()
                hashes = dict()
            else:
                print("Invalid bucket name: {0}".format(bucket))
                return(["INVALID_BUCKET", None])
        # we now have a (possibly empty) dict of hashes
        # see if we got lucky
        if (hash in hashes):
            # this is a dupe
            return(["DUPE_ENTRY", None])
        # ok, we need to make sure the cache is current
        filestoupdate = self._uncached_files(bucket)
        if (len(filestoupdate) > 0):
            self._hydrate_bucket(bucket, filestoupdate)
        hashes = self.HashDict[bucket]
        if (hash in hashes):
            # this is a dupe
            return(["DUPE_ENTRY", None])
        # OK, this is a new file, so add it to archive and update the HashDict
        # TODO
        return self._add_file_to_bucket(infile, bucket)
    
    @staticmethod
    def hash_file(file, bufsize = 262144):
        try:
            hash = hashlib.sha256()
            with open(file, 'rb') as f:
                while True:
                    data = f.read(bufsize)
                    if not data:
                        break
                    hash.update(data)
            return hash.hexdigest()
        except:
            return None
    
    @staticmethod
    def _is_valid_bucket(bucketname):
        # if we want to restrict bucket names, say to match the "yyyy\mm"
        # structure, we can add that filtering logic here. For now, allow
        # any name to pass.
        return True
    
    @staticmethod
    def makedir(pathname):
        #print("DEBUG: makedir called on pathname {0}".format(pathname))
        if os.path.isdir(pathname):
            return
        dirname = os.path.dirname(pathname)
        if dirname:
            ArchiveMgr.makedir(dirname)    # recurse
        os.mkdir(pathname, 0x0777)
    
    @staticmethod
    def file_exists(f, dir):
        return os.path.isfile(os.path.join(dir, os.path.basename(f)))
    
    @staticmethod
    def _gen_safe_filename(file, folder, addchar = '~'):
        # given a base file name, return unchanged if it doesn't exist in folder
        # If it does exist, append a tilde (~) to the end (before extension) until no files match it.
        if not ArchiveMgr.file_exists(os.path.basename(file), folder):
            return file
        else:
            base, ext = os.path.splitext(os.path.basename(file))
            newfile = base + addchar + ext
            return ArchiveMgr._gen_safe_filename(newfile, folder, addchar)
    
    def _add_file_to_bucket(self, infile, bucket):
        # generate unique file name and store it in bucket, return new file name
        fdfolder = safename = fqsafename = "*UNDEF*"    # in case we bomb before setting them in try block
        try:
            fqfolder = os.path.join(self.Root, bucket)
            #print("DEBUG:  In _add_file_to_bucket, fqfolder is {0}".format(fqfolder))
            safename = ArchiveMgr._gen_safe_filename(os.path.basename(infile), fqfolder)
            fqsafename = os.path.join(fqfolder, safename)
            shutil.copy2(infile, fqsafename)
            return ["SUCCESS", fqsafename]
        except Exception as e:
            print("Error copying file {0} as {1} to {2} -- {3}".format(infile, safename, fqfolder, e))
            return ["COPY_ERROR", None]
    
    def _hydrate_bucket(self, bucket, files):
        # Might be new bucket altogether
        if (bucket in self.HashDict):
            hdict = self.HashDict[bucket]
        else:
            hdict = dict()
        for file in files:
            fqfile = os.path.join(self.Root, bucket, file)
            hash = self.hash_file(fqfile)
            hdict[hash] = os.path.basename(fqfile)
        self.HashDict[bucket] = hdict   # update
    
    def _uncached_files(self, bucket):
        if (bucket in self.HashDict):
            setcachedfiles = set(self.HashDict[bucket].values())
        else:
            setcachedfiles = set()
        setdiskfiles = self._current_files_in_bucket(bucket)
        return setdiskfiles - setcachedfiles
    
    def _current_files_in_bucket(self, bucket):
        # return set of file names currently under root in specified bucket
        # Since we never delete files from archive, any files in this list
        # not in the cached HashDict represent files that need to be added
        try:
            folder = os.path.join(self.Root, bucket)
            #print("DEBUG: in _current_files_in_bucket, folder is {0}".format(folder))
            if (os.path.isdir(folder)):
                return set(os.listdir(folder))
            else:
                # new folder?
                ArchiveMgr.makedir(folder)
                return set()    # nothing there
        except Exception as e:
            print("Error getting _current_files_in_bucket({0}, {1}), returning null set -- {2}".format(self.Root, bucket, e))
            return set()
    

############################################################################
class PhotoIndexer(object):
    def __init__(self, root, spec= "**\\*.jpg"):
        self.picroot = root
        self.filterfn = None
        self.spec = spec
    
    def set_filterfn(self, fn):
        '''
            Set a file filter function for this instance
            :param: fn (function that takes a fully-qualified file name and returns a boolean)
        '''
        self.filterfn = fn
    
    def index_pics(self): 
        '''
        Given a location in self.picroot, a glob spec in self.spec, and a file filter
        function in self.filterfn, examine all qualifying photos in the tree. Hash and
        categorize them into yyyy\mm date buckets. Return a dictionary keyed by date bucket,
        with the bucket values being a list of entries. Each entry is a list of [filename, size, ymd, hash]
        :return: dictionary[bucket] = list([filename, size, ymd, hash])
        '''
        pics_by_date = {}
        count = 0
        for pic in glob.iglob(os.path.join(self.picroot, self.spec), recursive=True):
            if (self.filterfn == None or self.filterfn(pic)):   #run pic through filter function, only process if passes
                count += 1
                try:
                    size = os.stat(pic).st_size
                    ymd = self._image_date(pic)
                    bucket = self._bucket_from_date(ymd)  # key for dictionary (yyyy\mm)
                    fingerprint = self.hash_file(pic)
                    entry = [pic, size, ymd, fingerprint]
                    if bucket in pics_by_date:
                        pics_by_date[bucket].append(entry)
                    else:
                        pics_by_date[bucket] = [entry]
                except Exception as e:
                    print("Error examining file '{0}' -- {1}".format(pic, e))
                if (count % 100 == 0):
                    print("Indexing count: {0}".format(count))
        print("Total of {0} photo(s) indexed into {1} monthly bucket(s)".format(count, len(pics_by_date)))
        return pics_by_date
    
    @staticmethod
    def hash_file(file, bufsize = 262144):
        try:
            hash = hashlib.sha256()
            with open(file, 'rb') as f:
                while True:
                    data = f.read(bufsize)
                    if not data:
                        break
                    hash.update(data)
            return hash.hexdigest()
        except:
            return None
    
    def _truncate_to_hms(self, dt):
        if not isinstance(dt, datetime.datetime):
            raise ValueError("Non-datetime passed to truncate_to_hms")
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    
    def _try_strptime(self, s, format):
        """
        @param s the string to parse
        @param format the format to attempt parsing of the given string
        @return the parsed datetime or None on failure to parse 
        @see datetime.datetime.strptime
        """
        try:
            date = datetime.datetime.strptime(s, format)
        except ValueError:
            date = None
        return date
    
    def _parse_dt(self, dtstr):
        if isinstance(dtstr, datetime.datetime):
            return _truncate_to_hms(dtstr)
        if isinstance(dtstr, str):
            result = self._try_strptime(dtstr, "%Y-%m-%d %H:%M:%S") \
                    or self._try_strptime(dtstr, "%Y/%m/%d %H:%M:%S") \
                    or self._try_strptime(dtstr, "%Y:%m:%d %H:%M:%S")
            if result and isinstance(result, datetime.datetime):
                return self._truncate_to_hms(result)
        return None
        
    def _bucket_from_date(self, dt):
        if not isinstance(dt, datetime.datetime):
            print("_bucket_from_date passed a non-datetime [{0}] -- returning None".format(dt))
            return None
        return "{0:04}\\{1:02}".format(dt.year, dt.month)
    
    def _image_date(self, pic):
        stat = os.stat(pic)
        fsize = stat.st_size
        ctime = self._truncate_to_hms(datetime.datetime.fromtimestamp(stat.st_ctime))
        mtime = self._truncate_to_hms(datetime.datetime.fromtimestamp(stat.st_mtime))
        filetime = ctime if ctime < mtime else mtime    # create time can be later than mod time!
        img = PILimage.open(pic)
        image = ImageData(img)
        earliest_exif_date = self._parse_dt(image.earliest_date)
        date = filetime # TEMP
        date = earliest_exif_date or filetime   #EXIF data is considered authoritative
        ymd = datetime.datetime(date.year, date.month, date.day)
        img.close()
        return ymd
    
#############################################################################################

# Our basic filtering function
def ok_to_process(f):
    '''
        Boolean function used by index_pics to determine whether to process file f
    '''
    # return true if we want to exclude this file
    dir = os.path.dirname(f)
    # List of path substrings that we're not interested in
    excludes = ["Backup_Photos", ":\\Program Files", "RECYCLE.BIN", ":\\ProgramData", "\\INetCache", "\\cache",
                "\\AppData", "\\Windows", "\\CLIPART", "\\Paint Shop Pro 7", "\\WebTemplates", "\\Sample",
                "\\Visual Studio", "\\depot", "\\Device Stage", "\\Eclipse\\features", "\\All Users\\Adobe\\Elements",
                "\\All Users\\Adobe\\Photoshop Elements"]
    for ex in excludes:
        if ex.lower() in dir.lower():
            return False
    # if all pass, keep the file
    return True


def copy_indexed_pics_to_backup(pics, destroot=r"J:\Backup_Photos"):
    total_copied = 0
    am = ArchiveMgr(destroot)
    for bucket in pics.keys():
        nskipped = 0
        print("\nProcessing {0}".format(bucket))
        monthpics = pics[bucket]    # list of [filename, size, date, hash]
        copiedthisbucket = 0
        nrenamed = 0
        if (monthpics is None):
            print("No data found for monthly bucket {0}?".format(bucket))
            continue    # go to next month/bucket
        for picdata in monthpics:
            (fname, fsize, fdate, hash) = picdata
            result = am.submit_file_for_backup(fname, bucket, hash)
            if (result[1] is None):
                nskipped += 1
            else:
                copiedthisbucket += 1
                # was it renamed?
                baseIn = os.path.basename(fname)
                baseOut = os.path.basename(result[1])
                if (baseIn != baseOut):
                    nrenamed += 1
        if (copiedthisbucket > 0):
            print("Copied {0} file(s) to bucket {1}, {2} renamed".format(copiedthisbucket, bucket, nrenamed))
            total_copied += copiedthisbucket
        if (nskipped > 0):
            print("Skipped {0} file(s) that already existed in bucket {1}".format(nskipped, bucket))
    print("Total of {0} file(s) copied to backup".format(total_copied))


#########################################
#   Example of top-level backup function
#   Takes a source root location, a destination root location,
#   and a boolean file filter function. Indexes, hashes, and
#   copies distinct photos to destination in yyyy\mm buckets (subfolders)
#########################################
def backup_photos(fromroot, destroot, filterfn = ok_to_process):
    indexer = PhotoIndexer(fromroot)
    indexer.set_filterfn(filterfn)
    idx = indexer.index_pics()
    copy_indexed_pics_to_backup(idx, destroot)

try:
    import PIL
    import PIL.Image as PILimage
    from PIL import ImageDraw, ImageFont, ImageEnhance
    from PIL.ExifTags import TAGS, GPSTAGS
    import sys
    import os
    import shutil
    import glob
    import re
    import datetime
    import shutil
    import hashlib
    import pyodbc
except ImportError as err:
    exit(err)


####################   ImageData   ############################
class ImageData(object):
    '''
        Class to extract image EXIF data
    '''
    def __init__(self, img):
        self.img = img
        self.get_exif_data()
        self.lat = self.get_lat()
        self.lon = self.get_lon()
        self.date =self.get_date_time()
        self.origdate = self.get_orig_date_time()
        self.digidate = self.get_digi_date_time()
        self.earliest_date = self.get_earliest_exif_date()
        super(ImageData, self).__init__()
    
    @staticmethod
    def get_if_exist(data, key):
        if key in data:
            return data[key]
        return None
    
    @staticmethod
    def convert_to_degrees(value):
        """
            Helper function to convert the GPS coordinates
            stored in the EXIF to degrees in float format
        """
        d0 = value[0][0]
        d1 = value[0][1]
        d = float(d0) / float(d1)
        m0 = value[1][0]
        m1 = value[1][1]
        m = float(m0) / float(m1)
        
        s0 = value[2][0]
        s1 = value[2][1]
        s = float(s0) / float(s1)
        
        return d + (m / 60.0) + (s / 3600.0)
    
    def get_exif_data(self):
        """
            Returns a dictionary from the exif data of an PIL Image item. Also
            converts the GPS Tags
        """
        try:
            exif_data = {}
            info = self.img._getexif()
            if info:
                for tag, value in info.items():
                    decoded = TAGS.get(tag, tag)
                    if decoded == "GPSInfo":
                        gps_data = {}
                        for t in value:
                            sub_decoded = GPSTAGS.get(t, t)
                            gps_data[sub_decoded] = value[t]
                        exif_data[decoded] = gps_data
                    else:
                        exif_data[decoded] = value
            self.exif_data = exif_data
        except:
            self.exif_data = None
        # return exif_data
    
    def get_lat(self):
        """
            Returns the latitude and longitude, if available, from the 
            provided exif_data (obtained through get_exif_data above)
        """
        # print(exif_data)
        if self.exif_data and 'GPSInfo' in self.exif_data:
            gps_info = self.exif_data["GPSInfo"]
            gps_latitude = self.get_if_exist(gps_info, "GPSLatitude")
            gps_latitude_ref = self.get_if_exist(gps_info, 'GPSLatitudeRef')
            if gps_latitude and gps_latitude_ref:
                lat = self.convert_to_degrees(gps_latitude)
                if gps_latitude_ref != "N":
                    lat = 0 - lat
                lat = str(f"{lat:.{5}f}")
                return lat
        else:
            return None
    
    def get_lon(self):
        """
            Returns the latitude and longitude, if available, from the 
            provided exif_data (obtained through get_exif_data above)
        """
        # print(exif_data)
        if self.exif_data and 'GPSInfo' in self.exif_data:
            gps_info = self.exif_data["GPSInfo"]
            gps_longitude = self.get_if_exist(gps_info, 'GPSLongitude')
            gps_longitude_ref = self.get_if_exist(gps_info, 'GPSLongitudeRef')
            if gps_longitude and gps_longitude_ref:
                lon = self.convert_to_degrees(gps_longitude)
                if gps_longitude_ref != "E":
                    lon = 0 - lon
                lon = str(f"{lon:.{5}f}")
                return lon
        else:
            return None
    
    def get_date_time(self):
        if self.exif_data and 'DateTime' in self.exif_data:
            date_and_time = self.exif_data['DateTime']
            return date_and_time
    
    def get_orig_date_time(self):
        if self.exif_data and 'DateTimeOriginal' in self.exif_data:
            date_and_time = self.exif_data['DateTimeOriginal']
            return date_and_time
    
    def get_digi_date_time(self):
        if self.exif_data and 'DateTimeDigitized' in self.exif_data:
            date_and_time = self.exif_data['DateTimeDigitized']
            return date_and_time
    
    def get_earliest_exif_date(self):
        dt = self.get_date_time()
        odt = self.get_orig_date_time()
        ddt = self.get_digi_date_time()
        if (dt is None or (odt is not None and odt < dt)):
            dt = odt
        if (dt is None or (ddt is not None and ddt < dt)):
            dt = ddt
        return dt


####################   ArchiveMgr   #######################################
class ArchiveMgr(object):
    '''
        Class for managing archive folder tree, preventing duplicate entries, etc.
        It maintains a HashDict, which is a cached data structure reflecting the
        names and hashes of each file stored in each subfolder (yyyy\mm bucket).
        This cache is automatically rehydrated when a request is made and the
        files on disk do not reflect the current cache. This is relatively easy
        since we don't need to deal with deletions, just additions.
    '''
    NULLHASH = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    def __init__(self, root, hashdict=None):
        self.Root = root
        if (hashdict):
            self.HashDict = hashdict
        else:
            self.HashDict = dict()
    
    def submit_file_for_backup(self, infile, bucket, hash=None):
        '''
        The method will see if its cache is current, and if not (i.e., there
        are files on disk not in our HashDict), it will hash any unknown
        files and add them to HashDict. If the hash of the input file matches
        that of any file already archived, this is a duplicate, and will be
        rejected with DUPE_ENTRY. Otherwise, the file will be copied to the
        archive tree in the bucket specified (avoiding any name collisions),
        and the new file name and hash will be added to the cached HashDict.
        :param infile: fully-qualified name of file to add
        :param bucket: yyyy\mm bucket (subfolder) to archive under
        :param hash: file hash (optional), will hash ourselves if not provided
        :return: list of [status, stored_file_name
        '''
        # if hash is provided (perhaps we already knew it due to earlier
        # workflow), just use it, otherwise hash it ourselves.
        if (hash is None):
            hash = self.hash_file(infile)
        # Get hash dictionary for this bucket
        if (bucket in self.HashDict):
            hashes = self.HashDict[bucket]
        else:
            # this is a new yyyy\mm bucket, perhaps
            if (self._is_valid_bucket(bucket)):
                # create element in HashDict for this new bucket
                self.HashDict[bucket] = dict()
                hashes = dict()
            else:
                print("Invalid bucket name: {0}".format(bucket))
                return(["INVALID_BUCKET", None])
        # we now have a (possibly empty) dict of hashes
        # see if we got lucky
        if (hash in hashes):
            # this is a dupe
            return(["DUPE_ENTRY", None])
        # ok, we need to make sure the cache is current
        filestoupdate = self._uncached_files(bucket)
        if (len(filestoupdate) > 0):
            self._hydrate_bucket(bucket, filestoupdate)
        hashes = self.HashDict[bucket]
        if (hash in hashes):
            # this is a dupe
            return(["DUPE_ENTRY", None])
        # OK, this is a new file, so add it to archive and update the HashDict
        # TODO
        return self._add_file_to_bucket(infile, bucket)
    
    @staticmethod
    def hash_file(file, bufsize = 262144):
        try:
            hash = hashlib.sha256()
            with open(file, 'rb') as f:
                while True:
                    data = f.read(bufsize)
                    if not data:
                        break
                    hash.update(data)
            return hash.hexdigest()
        except:
            return None
    
    @staticmethod
    def _is_valid_bucket(bucketname):
        # if we want to restrict bucket names, say to match the "yyyy\mm"
        # structure, we can add that filtering logic here. For now, allow
        # any name to pass.
        return True
    
    @staticmethod
    def makedir(pathname):
        #print("DEBUG: makedir called on pathname {0}".format(pathname))
        if os.path.isdir(pathname):
            return
        dirname = os.path.dirname(pathname)
        if dirname:
            ArchiveMgr.makedir(dirname)    # recurse
        os.mkdir(pathname, 0x0777)
    
    @staticmethod
    def file_exists(f, dir):
        return os.path.isfile(os.path.join(dir, os.path.basename(f)))
    
    @staticmethod
    def _gen_safe_filename(file, folder, addchar = '~'):
        # given a base file name, return unchanged if it doesn't exist in folder
        # If it does exist, append a tilde (~) to the end (before extension) until no files match it.
        if not ArchiveMgr.file_exists(os.path.basename(file), folder):
            return file
        else:
            base, ext = os.path.splitext(os.path.basename(file))
            newfile = base + addchar + ext
            return ArchiveMgr._gen_safe_filename(newfile, folder, addchar)
    
    def _add_file_to_bucket(self, infile, bucket):
        # generate unique file name and store it in bucket, return new file name
        fdfolder = safename = fqsafename = "*UNDEF*"    # in case we bomb before setting them in try block
        try:
            fqfolder = os.path.join(self.Root, bucket)
            #print("DEBUG:  In _add_file_to_bucket, fqfolder is {0}".format(fqfolder))
            safename = ArchiveMgr._gen_safe_filename(os.path.basename(infile), fqfolder)
            fqsafename = os.path.join(fqfolder, safename)
            shutil.copy2(infile, fqsafename)
            return ["SUCCESS", fqsafename]
        except Exception as e:
            print("Error copying file {0} as {1} to {2} -- {3}".format(infile, safename, fqfolder, e))
            return ["COPY_ERROR", None]
    
    def _hydrate_bucket(self, bucket, files):
        # Might be new bucket altogether
        if (bucket in self.HashDict):
            hdict = self.HashDict[bucket]
        else:
            hdict = dict()
        for file in files:
            fqfile = os.path.join(self.Root, bucket, file)
            hash = self.hash_file(fqfile)
            hdict[hash] = os.path.basename(fqfile)
        self.HashDict[bucket] = hdict   # update
    
    def _uncached_files(self, bucket):
        if (bucket in self.HashDict):
            setcachedfiles = set(self.HashDict[bucket].values())
        else:
            setcachedfiles = set()
        setdiskfiles = self._current_files_in_bucket(bucket)
        return setdiskfiles - setcachedfiles
    
    def _current_files_in_bucket(self, bucket):
        # return set of file names currently under root in specified bucket
        # Since we never delete files from archive, any files in this list
        # not in the cached HashDict represent files that need to be added
        try:
            folder = os.path.join(self.Root, bucket)
            #print("DEBUG: in _current_files_in_bucket, folder is {0}".format(folder))
            if (os.path.isdir(folder)):
                return set(os.listdir(folder))
            else:
                # new folder?
                ArchiveMgr.makedir(folder)
                return set()    # nothing there
        except Exception as e:
            print("Error getting _current_files_in_bucket({0}, {1}), returning null set -- {2}".format(self.Root, bucket, e))
            return set()
    

####################   PhotoIndexer   ########################################################
class PhotoIndexer(object):
    def __init__(self, root, spec= "**\\*.jpg"):
        self.picroot = root
        self.filterfn = None
        self.spec = spec
    
    def set_filterfn(self, fn):
        '''
            Set a file filter function for this instance
            :param: fn (function that takes a fully-qualified file name and returns a boolean)
        '''
        self.filterfn = fn
    
    def index_pics(self): 
        '''
        Given a location in self.picroot, a glob spec in self.spec, and a file filter
        function in self.filterfn, examine all qualifying photos in the tree. Hash and
        categorize them into yyyy\mm date buckets. Return a dictionary keyed by date bucket,
        with the bucket values being a list of entries. Each entry is a list of [filename, size, ymd, hash]
        :return: dictionary[bucket] = list([filename, size, ymd, hash])
        '''
        pics_by_date = {}
        count = 0
        for pic in glob.iglob(os.path.join(self.picroot, self.spec), recursive=True):
            if (self.filterfn == None or self.filterfn(pic)):   #run pic through filter function, only process if passes
                count += 1
                try:
                    size = os.stat(pic).st_size
                    ymd = self._image_date(pic)
                    bucket = self._bucket_from_date(ymd)  # key for dictionary (yyyy\mm)
                    fingerprint = self.hash_file(pic)
                    entry = [pic, size, ymd, fingerprint]
                    if bucket in pics_by_date:
                        pics_by_date[bucket].append(entry)
                    else:
                        pics_by_date[bucket] = [entry]
                except Exception as e:
                    print("Error examining file '{0}' -- {1}".format(pic, e))
                if (count % 100 == 0):
                    print("Indexing count: {0}".format(count))
        print("Total of {0} photo(s) indexed into {1} monthly bucket(s)".format(count, len(pics_by_date)))
        return pics_by_date
    
    @staticmethod
    def hash_file(file, bufsize = 262144):
        try:
            hash = hashlib.sha256()
            with open(file, 'rb') as f:
                while True:
                    data = f.read(bufsize)
                    if not data:
                        break
                    hash.update(data)
            return hash.hexdigest()
        except:
            return None
    
    def _truncate_to_hms(self, dt):
        if not isinstance(dt, datetime.datetime):
            raise ValueError("Non-datetime passed to truncate_to_hms")
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
    
    def _try_strptime(self, s, format):
        """
        @param s the string to parse
        @param format the format to attempt parsing of the given string
        @return the parsed datetime or None on failure to parse 
        @see datetime.datetime.strptime
        """
        try:
            date = datetime.datetime.strptime(s, format)
        except ValueError:
            date = None
        return date
    
    def _parse_dt(self, dtstr):
        if isinstance(dtstr, datetime.datetime):
            return _truncate_to_hms(dtstr)
        if isinstance(dtstr, str):
            result = self._try_strptime(dtstr, "%Y-%m-%d %H:%M:%S") \
                    or self._try_strptime(dtstr, "%Y/%m/%d %H:%M:%S") \
                    or self._try_strptime(dtstr, "%Y:%m:%d %H:%M:%S")
            if result and isinstance(result, datetime.datetime):
                return self._truncate_to_hms(result)
        return None
        
    def _bucket_from_date(self, dt):
        if not isinstance(dt, datetime.datetime):
            print("_bucket_from_date passed a non-datetime [{0}] -- returning None".format(dt))
            return None
        return "{0:04}\\{1:02}".format(dt.year, dt.month)
    
    def _image_date(self, pic):
        stat = os.stat(pic)
        fsize = stat.st_size
        ctime = self._truncate_to_hms(datetime.datetime.fromtimestamp(stat.st_ctime))
        mtime = self._truncate_to_hms(datetime.datetime.fromtimestamp(stat.st_mtime))
        filetime = ctime if ctime < mtime else mtime    # create time can be later than mod time!
        img = PILimage.open(pic)
        image = ImageData(img)
        earliest_exif_date = self._parse_dt(image.earliest_date)
        date = filetime # TEMP
        date = earliest_exif_date or filetime   #EXIF data is considered authoritative
        ymd = datetime.datetime(date.year, date.month, date.day)
        img.close()
        return ymd
    
#############################################################################################

# Our basic filtering function
def ok_to_process(f):
    '''
        Boolean function used by index_pics to determine whether to process file f
    '''
    try:
        # return true if we want to exclude this file
        dir = os.path.dirname(f)
        # List of path substrings that we're not interested in
        excludes = ["Backup_Photos", ":\\Program Files", "RECYCLE.BIN", ":\\ProgramData", "\\INetCache", "\\cache",
                    "\\AppData", "\\Windows", "\\CLIPART", "\\Paint Shop Pro 7", "\\WebTemplates", "\\Sample",
                    "\\Visual Studio", "\\depot", "\\Device Stage", "\\Eclipse\\features", "\\All Users\\Adobe\\Elements",
                    "\\All Users\\Adobe\\Photoshop Elements"]
        for ex in excludes:
            if ex.lower() in dir.lower():
                return False
        # skip AlbumArt files
        base = os.path.basename(f).upper()
        if base.startswith("ALBUMART"):
            return False
        # if all pass, keep the file
        return True
    except:
        print("ERROR: Filter function ok_to_process failed on passed file \"{0}\", returned False".format(f))
        return False

def copy_indexed_pics_to_backup(pics, destroot):
    total_copied = 0
    am = ArchiveMgr(destroot)
    for bucket in pics:
        nskipped = 0
        print("\nProcessing {0}".format(bucket))
        monthpics = pics[bucket]    # list of [filename, size, date, hash]
        copiedthisbucket = 0
        nrenamed = 0
        if (monthpics is None):
            print("No data found for monthly bucket {0}?".format(bucket))
            continue    # go to next month/bucket
        for picdata in monthpics:
            (fname, fsize, fdate, hash) = picdata
            result = am.submit_file_for_backup(fname, bucket, hash)
            if (result[1] is None):
                nskipped += 1
            else:
                copiedthisbucket += 1
                # was it renamed?
                baseIn = os.path.basename(fname)
                baseOut = os.path.basename(result[1])
                if (baseIn != baseOut):
                    nrenamed += 1
        if (copiedthisbucket > 0):
            print("Copied {0} file(s) to bucket {1}, {2} renamed".format(copiedthisbucket, bucket, nrenamed))
            total_copied += copiedthisbucket
        if (nskipped > 0):
            print("Skipped {0} file(s) that already existed in bucket {1}".format(nskipped, bucket))
    print("Total of {0} file(s) copied to backup".format(total_copied))


#########################################
#   Top-level backup function, takes a source root location, a destination
#   root location, and a boolean file filter function. Indexes, hashes,
#   and copies distinct photos to destination in yyyy\mm buckets (subfolders)
#########################################
def backup_photos(fromroot, destroot, filterfn = ok_to_process):
    indexer = PhotoIndexer(fromroot)
    indexer.set_filterfn(filterfn)
    idx = indexer.index_pics()
    copy_indexed_pics_to_backup(idx, destroot)

# example invocation:
# backup_photos(fromroot="C:\\", destroot="J:\\Backup_Photos", filterfn=ok_to_process)
