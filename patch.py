
import database as db
import logSetup
logSetup.initLogging()

import danbooruFetch
import runstate
import settings
import concurrent.futures

import os
import os.path
import hashlib

THREADS = 1
THREADS = 25

def insertDanbooruStartingPoints():

	tmp = db.session.query(db.Releases) \
		.filter(db.Releases.postid == 1)  \
		.count()
	if not tmp:
		for x in range(2070000):
			new = db.Releases(dlstate=0, postid=x, source='Danbooru')
			db.session.add(new)
			if x % 10000 == 0:
				print("Loop ", x, "flushing...")
				db.session.flush()
				print("Flushed.")
	db.session.commit()


def resetDlstate():
	tmp = db.session.query(db.Releases) \
	.filter(db.Releases.dlstate == 1)   \
	.update({db.Releases.dlstate : 0})
	db.session.commit()


def go():
	have = db.session.query(db.Releases)             \
		.filter(db.Releases.filepath != None) \
		.all()

	proc = 0
	for row in have:
		fpath = settings.storeDir+row.filepath
		fpath = os.path.abspath(fpath)

		with open(fpath, "rb") as fp:
			cont = fp.read()
			fhash = hashlib.md5(cont).hexdigest()


		print(os.path.exists(fpath), fhash, fpath)

		row.file.append((fpath, fhash))
		proc += 1
		if proc % 50 == 0:
			db.session.commit()
if __name__ == '__main__':
	go()

