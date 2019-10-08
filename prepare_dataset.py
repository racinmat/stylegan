from collections import defaultdict
import shutil
import numpy as np
import os
import os.path as osp
import pandas as pd
from PIL import Image
import sys
import hashlib

raw_imgs_path = 'DanbooruDownloader/downloaded'
cleaned_imgs_path = 'DanbooruDownloader/deduplicated'


def chunk_reader(fobj, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, hash_fn=hashlib.sha1):
    hashobj = hash_fn()
    file_object = open(filename, 'rb')

    for chunk in chunk_reader(file_object):
        hashobj.update(chunk)
    hashed = hashobj.digest()
    file_object.close()

    return hashed


def deduplicate_images(files_path, hash_fn=hashlib.sha1):
    hashes_full = defaultdict(list)
    duplicate_count = 0

    for dirpath, dirnames, filenames in os.walk(files_path):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            full_path = os.path.realpath(full_path)
            full_hash = get_hash(full_path, hash_fn)
            duplicate = hashes_full.get(full_hash)
            if duplicate:
                # print("Duplicate found: %s and %s" % (filename, duplicate))
                duplicate_count += 1
            hashes_full[full_hash].append(full_path)

    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
    print(f"{duplicate_count} duplicates found")
    for files in hashes_full.values():
        shutil.copyfile(files[0], osp.join(cleaned_imgs_path, osp.basename(files[0])))
    print('done copying non-duplicated files')


if __name__ == '__main__':
    deduplicate_images(raw_imgs_path, hash_fn=hashlib.sha1)

