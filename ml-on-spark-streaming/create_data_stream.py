import glob, os, shutil
import time

source_dir = '/Users/Yogi/Documents/spark_streming_meetup/data'
dst = '/Users/Yogi/Documents/spark_streming_meetup/source'
files = glob.iglob(os.path.join(source_dir, "*"))
for file in files:
    if os.path.isfile(file):
        shutil.copy2(file, dst)
    time.sleep(30)
